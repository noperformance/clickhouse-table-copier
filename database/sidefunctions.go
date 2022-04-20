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

func ReturnValuesString(source []map[string]interface{}, destination []map[string]interface{}) string {

	var (
		res string
		tmp []string
	)

	for _, sourceMap := range source {
		for _, destinationMap := range destination {
			if sourceMap["name"] == destinationMap["name"] {
				tmp = append(tmp, fmt.Sprintf("%v", sourceMap["name"]))
			}
		}
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

func ReturnDestValues(source []map[string]interface{}, destination []map[string]interface{}) string {

	var (
		tmp []string
		res string
	)
	for _, sourceMap := range source {
		for _, destinationMap := range destination {
			if sourceMap["name"] == destinationMap["name"] {
				if sourceMap["type"] == destinationMap["type"] {
					tmp = append(tmp, fmt.Sprintf("%v", sourceMap["name"]))
				} else {
					switch sourceMap["type"] {
					case "String":
						switch destinationMap["type"] {
						case "IPv4":
							tmp = append(tmp, fmt.Sprintf("toIPv4(%v)", sourceMap["name"]))
						case "IPv6":
							tmp = append(tmp, fmt.Sprintf("toIPv6(%v)", sourceMap["name"]))
						case "UUID":
							tmp = append(tmp, fmt.Sprintf("toUUIDOrZero(%v)", sourceMap["name"]))
						case "JSON":
							tmp = append(tmp, fmt.Sprintf("toJSONString(%v)", sourceMap["name"]))
						case "Int8":
							tmp = append(tmp, fmt.Sprintf("toInt8(%v)", sourceMap["name"]))
						case "Int16":
							tmp = append(tmp, fmt.Sprintf("toInt16(%v)", sourceMap["name"]))
						case "Int32":
							tmp = append(tmp, fmt.Sprintf("toInt32(%v)", sourceMap["name"]))
						case "Int64":
							tmp = append(tmp, fmt.Sprintf("toInt64(%v)", sourceMap["name"]))
						case "Int128":
							tmp = append(tmp, fmt.Sprintf("toInt128(%v)", sourceMap["name"]))
						case "Int256":
							tmp = append(tmp, fmt.Sprintf("toInt256(%v)", sourceMap["name"]))
						case "Date":
							tmp = append(tmp, fmt.Sprintf("toDate(%v)", sourceMap["name"]))
						case "DateTime":
							tmp = append(tmp, fmt.Sprintf("toDateTime(%v)", sourceMap["name"]))
						case "LowCardinality(String)":
							tmp = append(tmp, fmt.Sprintf("%v", sourceMap["name"])) // оставим на CAST
						default:
							log.Debugf("[source name] : %v | [source type] : %v  | [dest name] : %v | [dest type] : %v", sourceMap["name"], sourceMap["type"], destinationMap["name"], destinationMap["type"])
						}
					case "Array(String)":
						switch destinationMap["type"] {
						case "Array(LowCardinality(String))":
							tmp = append(tmp, fmt.Sprintf("%v", sourceMap["name"])) // оставим на CAST
						default:
							log.Debugf("[source name] : %v | [source type] : %v  | [dest name] : %v | [dest type] : %v", sourceMap["name"], sourceMap["type"], destinationMap["name"], destinationMap["type"])
						}
					case "Int32":
						switch destinationMap["type"] {
						case "UInt32":
							tmp = append(tmp, fmt.Sprintf("toUInt32(%v)", sourceMap["name"]))
						case "UInt16":
							tmp = append(tmp, fmt.Sprintf("toUInt16(%v)", sourceMap["name"]))
						case "UInt8":
							tmp = append(tmp, fmt.Sprintf("toUInt8(%v)", sourceMap["name"]))
						default:
							log.Debugf("[source name] : %v | [source type] : %v  | [dest name] : %v | [dest type] : %v", sourceMap["name"], sourceMap["type"], destinationMap["name"], destinationMap["type"])
						}

					default:
						switch destinationMap["type"] {
						case "JSON":
							tmp = append(tmp, fmt.Sprintf("toJSONString(%v)", sourceMap["name"]))
						default:
							log.Debugf("[source name] : %v | [source type] : %v  | [dest name] : %v | [dest type] : %v", sourceMap["name"], sourceMap["type"], destinationMap["name"], destinationMap["type"])
						}

					}
				}
			}
		}
	}

	res = strings.Join(tmp, ",")

	return res
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
