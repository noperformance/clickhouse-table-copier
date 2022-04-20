package database

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"time"
)

func RegexPartitionName(name interface{}) []string {
	r := regexp.MustCompile(`([a-zA-Z0-9-:_])+`)
	res := r.FindAllString(fmt.Sprintf("%v", name), -1)
	return res
}

func RegexPartitionKeysFromSettings(keys interface{}, setting tableSettings) []string {

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

func RegexPartitionKeysFromSettingsRaw(keys interface{}, setting tableSettings) []string {

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

func PartitionKeysToMap(keys []string, settings tableSettings) map[int]map[string]string {

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

//func IfTimeStampInPartitionKeys(keys map[string]string) bool {
//	for _, value := range keys {
//		if value == "DateTime" {
//			return true
//		}
//	}
//	return false
//}

func ReturnValuesString(desc []map[string]interface{}) string {

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

func GenerateWhere(from interface{}, to interface{}, partitionKeys map[int]map[string]string, partitionName []string, rawPartitionKeys []string, complexPartitionKey bool) string {
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
