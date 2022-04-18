package database

import (
	"clickhouse-table-copier/config"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	once         sync.Once
	chdbInstance *ChDb
)

type ChDb struct {
	dsn       string
	connect   *sql.DB
	reconnect bool
	mux       sync.Mutex
	log       *log.Logger
}

func New() *ChDb {

	once.Do(func() {
		chdbInstance = new(ChDb)
	})
	return chdbInstance
}

type dsnString struct {
	Host string
	Port string
	args map[string]interface{}
}

type tableSettings struct {
	TableName       string
	DbName          string
	IsTableExist    bool
	RowsCount       uint64
	Describe        []map[string]interface{}
	Info            []map[string]interface{}
	TablePartitions []map[string]interface{}
}

func (ch *ChDb) CreateTableSettings(DatabaseName string, TableName string) tableSettings {
	return tableSettings{
		TableName:       TableName,
		DbName:          DatabaseName,
		IsTableExist:    ch.CheckIfTableExist(DatabaseName, TableName),
		Describe:        ch.GetTableDescribe(DatabaseName, TableName),
		Info:            ch.GetTableInfo(DatabaseName, TableName),
		RowsCount:       ch.CheckRowsCount(DatabaseName, TableName),
		TablePartitions: ch.GetTablePartitions(DatabaseName, TableName),
	}
}

func CreateDsnString(host string, port uint16) dsnString {
	if len(host) < 1 {
		host = "localhost"
	}
	if port < 1 {
		port = 9000
	}
	return dsnString{
		Host: host,
		Port: strconv.FormatUint(uint64(port), 10),
		args: map[string]interface{}{},
	}
}

func (d dsnString) Add(argName string, argValue interface{}) {
	d.args[argName] = argValue
}

func (d dsnString) GetDSN() string {
	result := fmt.Sprintf("tcp://%s:%s", d.Host, d.Port)
	if len(d.args) < 1 {
		return result
	}
	delim := "?"
	for k, v := range d.args {
		switch v.(type) {
		case string:
			if len(v.(string)) > 0 {
				result += fmt.Sprintf("%s%s=%v", delim, k, v)
			} else {
				continue
			}
		case bool:
			if v.(bool) {
				result += fmt.Sprintf("%s%s=%v", delim, k, v)
			} else {
				continue
			}
		default:
			result += fmt.Sprintf("%s%s=%v", delim, k, v)
		}
		delim = "&"
	}
	return result
}

func (ch *ChDb) SetDSN(dsn config.Connection) {
	dsnStr := CreateDsnString(dsn.HostName, dsn.Port)
	dsnStr.Add("username", dsn.UserName)
	dsnStr.Add("password", dsn.Password)
	dsnStr.Add("secure", dsn.Secure)
	dsnStr.Add("skip_verify", dsn.SkipVerify)
	ch.dsn = dsnStr.GetDSN()
}

func (ch *ChDb) QueryToNestedMap(query string) ([]map[string]interface{}, error) {

	var (
		result []map[string]interface{}
		values []interface{}
		args   []interface{}
	)

	rows, err := ch.Query(query)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return result, err
	}

	numOfCols := len(cols)

	for rows.Next() {

		row := make(map[string]interface{}, numOfCols)
		values = make([]interface{}, numOfCols)
		args = make([]interface{}, numOfCols)

		for col := 0; col < numOfCols; col++ {
			args[col] = &values[col]
		}

		if rows.Scan(args...); err != nil {
			return nil, err
		}

		for i, v := range values {
			switch v := v.(type) {
			case []byte:
				row[cols[i]] = string(v)
			default:
				row[cols[i]] = v
			}
		}
		result = append(result, row)
	}

	return result, nil
}

func (ch *ChDb) Close() error {
	if ch.connect != nil {
		err := ch.connect.Close()
		if err == nil {
			ch.connect = nil
		}
		return err
	}
	return nil
}

func (ch *ChDb) ReConnect() error {
	if ch.connect != nil {
		if err := ch.connect.Ping(); err == nil {
			return nil
		} else {
			if exception, ok := err.(*clickhouse.Exception); ok {
				log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			} else {
				log.Println(err)
			}
			err = ch.Close()
			if err != nil {
				log.Printf("Database error: %s", err)
			}
		}
	}
	connect, err := sql.Open("clickhouse", ch.dsn)
	if err != nil {
		return err
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Println(err)
		}
		return err
	}
	ch.connect = connect
	return nil
}

func (ch *ChDb) ReConnectLoop() error {
	for {
		err := ch.ReConnect()
		if err != nil {
			log.Printf("Error connect to Clickhouse: %s. Sleep 5s", err)
			time.Sleep(time.Second * 5)
			continue
		}
		return nil
	}
}

func (ch *ChDb) Execute(q string) (sql.Result, error) {
	ch.mux.Lock()
	defer ch.mux.Unlock()
	err := ch.ReConnectLoop()
	if err != nil {
		return nil, err
	}
	log.Debugf("[sql] execute: %s | dsn: %v", q, ch.dsn)
	return ch.connect.Exec(q)
}
func (ch *ChDb) Query(q string) (*sql.Rows, error) {
	ch.mux.Lock()
	defer ch.mux.Unlock()
	err := ch.ReConnectLoop()
	if err != nil {
		return nil, err
	}
	log.Debugf("[sql] query: %s | dsn: %v", q, ch.dsn)
	return ch.connect.Query(q)
}

func (ch *ChDb) GetTimezone() (string, error) {
	out, err := ch.Query(`SELECT timezone()`)
	if err != nil {
		return "", err
	}
	var res string
	defer ch.Close()

	for out.Next() {
		var (
			result string
		)
		if err := out.Scan(&result); err != nil {
			log.Fatal(err)
		}
		res = result
	}

	return res, err
}

func (ch *ChDb) GetTablePartitions(database string, table string) []map[string]interface{} {

	out, err := ch.QueryToNestedMap(fmt.Sprintf(`
			SELECT
				partition as partition,
				name as name,
				active	as active,
				partition_id as partition_id,
				min_time as min_time,
				max_time as max_time,
				formatReadableSize(sum(bytes)) as size,
				sum(rows) as rows,
				max(modification_time) as latest_modification,
				sum(bytes)	as bytes_size,
				any(engine) as engine,
				formatReadableSize(sum(primary_key_bytes_in_memory)) as primary_keys_size
			FROM system.parts
			WHERE table = '%s' and database = '%s'
			GROUP BY partition,name,active,partition_id,min_time,max_time
			ORDER BY bytes_size DESC`, table, database))

	if err != nil {
		log.Fatal(err)
	}

	return out

}

func (ch *ChDb) GetTableDescribe(database string, table string) []map[string]interface{} {

	out, err := ch.QueryToNestedMap(fmt.Sprintf("describe table %s.%s", database, table))
	if err != nil {
		log.Fatal("cant get description, ", err)
	}

	return out
}

func (ch *ChDb) GetTableInfo(database string, table string) []map[string]interface{} {

	out, err := ch.QueryToNestedMap(fmt.Sprintf("SELECT * FROM system.tables WHERE database='%s' AND name='%s'", database, table))
	if err != nil {
		log.Fatal("cant get table info, ", err)
	}

	return out
}

func (ch *ChDb) CheckIfTableExist(database string, table string) bool {

	out, err := ch.Query(fmt.Sprintf(`EXISTS ` + database + `.` + table))
	if err != nil {
		log.Fatal("can check table exists, ", err)
	}

	var res uint8
	i2b := []bool{false, true}
	defer ch.Close()

	for out.Next() {
		var (
			result uint8
		)
		if err := out.Scan(&result); err != nil {
			log.Fatal("cant scan")
		}
		res = result
	}

	return i2b[res]
}

func (ch *ChDb) CheckRowsCount(database string, table string) uint64 {

	out, err := ch.Query(fmt.Sprintf(`SELECT count(*) FROM ` + database + `.` + table))
	if err != nil {
		log.Fatal("cant select rows count, ", err)
	}

	var res uint64
	defer ch.Close()

	for out.Next() {
		var (
			result uint64
		)
		if err := out.Scan(&result); err != nil {
			log.Fatal("cant scan rows")
		}
		res = result
	}

	return res

}
func (ch *ChDb) RegexPartitionName(name interface{}) []string {
	r := regexp.MustCompile(`([a-zA-Z0-9-:_])+`)
	res := r.FindAllString(fmt.Sprintf("%v", name), -1)
	return res
}

func (ch *ChDb) RegexPartitionKeysFromSettings(keys interface{}, setting tableSettings) []string {

	tmp := regexp.MustCompile(`([a-zA-Z_:])+`).FindAllString(fmt.Sprintf("%v", keys), -1)

	var res []string

	for _, value := range setting.Describe {
		for _, key := range tmp {
			if fmt.Sprintf("%v", value["name"]) == key {
				res = append(res, key)
			}
		}
	}

	return res
}

func (ch *ChDb) RegexPartitionKeysFromSettingsRaw(keys interface{}, setting tableSettings) []string {

	first := regexp.MustCompile(`[^a-zA-Z]\((.*)\)`).ReplaceAllString(fmt.Sprintf("%v", keys), "${1}")
	tmp := strings.Split(first, ",")

	var res []string

	for _, value := range setting.Describe {
		for _, key := range tmp {
			if strings.Contains(key, fmt.Sprintf("%v", value["name"])) {
				res = append(res, key)
			}
		}
	}

	return res
}

// почему так? а потому что голанг своим := range выбирает рандомные элементы а не по порядку эх
func (ch *ChDb) PartitionKeysToMap(keys []string, settings tableSettings) map[int]map[string]string {

	res := make(map[int]map[string]string)

	for index, key := range keys {
		for _, desc := range settings.Describe {
			if desc["name"] == key {
				tmp := make(map[string]string)
				tmp[fmt.Sprintf("%v", desc["name"])] = fmt.Sprintf("%v", desc["type"])
				res[index] = tmp
			}
		}
	}

	return res
}

func (ch *ChDb) IfTimeStampInPartitionKeys(keys map[string]string) bool {
	for _, value := range keys {
		if value == "DateTime" {
			return true
		}
	}
	return false
}

func (ch *ChDb) ReturnValuesString(desc []map[string]interface{}) string {

	var (
		res string
		tmp []string
	)

	for _, row := range desc {
		tmp = append(tmp, fmt.Sprintf("%v", row["name"]))
	}

	res = strings.Join(tmp, ",")

	return res

}

func timeRange(from interface{}, to interface{}) (string, string) {

	layout := "2006-01-02 15:04:05 Z0700 MST"

	parseFrom, _ := time.Parse(layout, fmt.Sprintf("%v", from))
	parseTo, _ := time.Parse(layout, fmt.Sprintf("%v", to))

	timeFrom := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", parseFrom.Year(), parseFrom.Month(), parseFrom.Day(), parseFrom.Hour(), parseFrom.Minute(), parseFrom.Second())
	timeTo := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", parseTo.Year(), parseTo.Month(), parseTo.Day(), parseTo.Hour(), parseTo.Minute(), parseTo.Second())

	return timeFrom, timeTo
}

func (ch *ChDb) GenerateWhere(from interface{}, to interface{}, partitionKeys map[int]map[string]string, partitionName []string, rawPartitionKeys []string, complexPartitionKey bool) string {
	var (
		where string
		tmp   int
	)

	if complexPartitionKey {
		log.Print("complex partition keys")
	}

	where = "WHERE "
	tmp = 0

	timeFrom, timeTo := timeRange(from, to)

	//голанг не умеет в range!
	for index, stringMap := range partitionKeys {
		for name, colType := range stringMap {
			for nameIndex, nameVal := range partitionName {

				if index == nameIndex {
					if colType == "DateTime" || colType == "Date" {
						if timeFrom != "1970-01-01 03:00:00" && timeTo != "1970-01-01 03:00:00" {
							where = where + fmt.Sprintf("%s >= toDateTime('%v') AND %v <= toDateTime('%v')", name, timeFrom, name, timeTo)
						} else {
							log.Print(rawPartitionKeys[index])
							where = where + fmt.Sprintf("%s='%s'", rawPartitionKeys[index], nameVal)
						}
					} else {
						where = where + fmt.Sprintf("%s='%s'", name, nameVal)
					}
				}
			}

			if tmp < len(partitionKeys)-1 {
				where = where + " AND "
			}

			tmp = +1
		}
	}

	return where
}

func (ch *ChDb) DeletePartition(destinationTable tableSettings, where string) bool {

	_, err := ch.Execute(fmt.Sprintf("ALTER TABLE %s.%s DELETE %s",
		destinationTable.DbName, destinationTable.TableName, where))
	if err != nil {
		log.Fatal("cant delete rows!", err)
	}

	return true
}

func (ch *ChDb) CopyPartition(sourceTable tableSettings, destinationTable tableSettings, sourceConfiguration config.Connection, values string, where string) bool {

	_, err := ch.Execute(fmt.Sprintf("INSERT INTO %s.%s (%s) SELECT %s FROM remote('%s:%v', %s, %s, '%s','%s') %s",
		destinationTable.DbName, destinationTable.TableName, values, values, sourceConfiguration.HostName, sourceConfiguration.Port, sourceTable.DbName, sourceTable.TableName, sourceConfiguration.UserName, sourceConfiguration.Password, where))
	if err != nil {
		log.Fatal("cant copy partition", err)
	}

	return true
}

func (ch *ChDb) PartitionHashCheck(table tableSettings, values string, where string) uint64 {

	out, err := ch.Query(fmt.Sprintf("SELECT groupBitXor(cityHash64(*)) FROM (SELECT %s FROM %s.%s %s)",
		values, table.DbName, table.TableName, where))
	if err != nil {
		log.Fatal("cant check hash timestamp", err)
	}

	var res uint64

	for out.Next() {
		var (
			result uint64
		)
		if err := out.Scan(&result); err != nil {
			log.Fatal("cant scan rows", err)
		}
		log.Print(result)
		res = result
	}

	return res
}
