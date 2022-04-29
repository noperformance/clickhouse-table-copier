package copy

import (
	"clickhouse-table-copier/config"
	"clickhouse-table-copier/database"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func Info() {

	c := config.New()

	source := *database.New()
	source.SetDSN(c.SourceConnection)
	sourceTableSettings := source.CreateTableSettings(c.SourceConnection.Database, c.SourceConnection.Table)

	destination := *database.New()
	destination.SetDSN(c.DestinationConnection)
	destinationTableSettings := destination.CreateTableSettings(c.DestinationConnection.Database, c.DestinationConnection.Table)

	sourceTimezone, _ := source.GetTimezone()
	destinationTimezone, _ := destination.GetTimezone()

	if sourceTimezone != destinationTimezone {
		log.Fatal("different timezones!")
	} else {
		loc, _ := time.LoadLocation(sourceTimezone)
		time.Local = loc
		log.Debug("Timezone set to: " + sourceTimezone)
	}

	if sourceTableSettings.RowsCount == destinationTableSettings.RowsCount {
		log.Warn("[main] Row count in both tables same!")
		log.Warn("[main] Could be a problem")
	} else if sourceTableSettings.RowsCount < destinationTableSettings.RowsCount {
		log.Warn("[main] Row count in destination table more than in source table!")
		log.Warn("[main] Could be a problem")
	}

	var (
		sourceComapre              = make(chan uint64)
		destinationCompare         = make(chan uint64)
		sourcePartitionHash        uint64
		destinationPartitionHash   uint64
		partitionName              = []string{""}
		partitionKeysWithFunctions = false
		wg                         sync.WaitGroup
	)

	log.Debug("[main] Getting partition keys")
	sourcePartitionKeyStringArray := database.RegexPartitionKeysFromSettings(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyStringArrayRaw := database.RegexPartitionKeysFromSettingsRaw(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyMap := database.PartitionKeysToMap(sourcePartitionKeyStringArray, sourceTableSettings)
	log.Debug("[main] Done..")

	if len(sourcePartitionKeyStringArray) == 0 {
		log.Fatal("[main] No partition keys!")
	}

	values := database.ReturnValuesString(sourceTableSettings.Describe, destinationTableSettings.Describe)
	destinationValues := database.ReturnDestValues(sourceTableSettings.Describe, destinationTableSettings.Describe)

	for _, partition := range sourceTableSettings.TablePartitions {

		if len(sourcePartitionKeyStringArray) > 1 {
			partitionName = database.RegexPartitionName(partition["partition"])
		} else {
			partitionName[0] = fmt.Sprintf("%v", partition["partition"])
		}

		log.Printf("[%v] Generating Where", partition["partition"])
		if len(sourcePartitionKeyStringArrayRaw) == len(sourcePartitionKeyStringArray) {
			partitionKeysWithFunctions = true
		}
		currentWhere := database.GenerateWhere(partition["min_time"], partition["max_time"], sourcePartitionKeyMap, partitionName, sourcePartitionKeyStringArrayRaw, partitionKeysWithFunctions)

		if destination.CheckPartitionRowCount(currentWhere, destinationTableSettings) != 0 { //если  удаленная партиция пустая то и хэши сверять незачем

			wg.Add(1)

			if c.CheckHashes {

				go source.PartitionHashCheckAsync(sourceTableSettings, destinationValues, currentWhere, sourceComapre, &wg)
				go destination.PartitionHashCheckAsync(destinationTableSettings, values, currentWhere, destinationCompare, &wg)

			} else {

				go source.CheckPartitionRowCountAsync(currentWhere, sourceTableSettings, sourceComapre, &wg)
				go destination.CheckPartitionRowCountAsync(currentWhere, destinationTableSettings, destinationCompare, &wg)

			}

			sourcePartitionHash = <-sourceComapre
			destinationPartitionHash = <-destinationCompare

			log.Debug(sourcePartitionHash)
			log.Debug(destinationPartitionHash)

			if sourcePartitionHash == 0 && destinationPartitionHash == 0 {
				log.Fatal("hashes empty, looks like beda")
			}

			if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash == 0 {
				log.Printf("[%v] Destination partition empty! lets start copy partition", partition["name"])
				//go destination.CopyPartitionAsync(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, destinationValues, currentWhere, &wg)
				log.Printf("[%v] Copy of partition is finished! ", partition["name"])
			} else if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash != 0 {
				//wg.Add(1)
				log.Printf("[%v] Destination data inconsistent! remove needed", partition["name"])
				log.Printf("[%v] Removing...", partition["name"])
				//go destination.DeletePartitionAsync(destinationTableSettings, currentWhere, &wg)
				log.Printf("[%v] Remove finished", partition["name"])
				log.Printf("[%v] Starting to copy: ", partition["name"])
				//go destination.CopyPartitionAsync(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, destinationValues, currentWhere, &wg)
				log.Printf("[%v] Finished", partition["name"])
			} else {
				log.Printf("[%v] Partitions identical ! skipping ", partition["name"])
			}

		} else {
			//wg.Add(1)
			log.Printf("[%v] Destination partition row count is zero, so we just starting copy partition: ", partition["name"])
			//go destination.CopyPartitionAsync(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, destinationValues, currentWhere, &wg)
			log.Printf("[%v] Copy of partition is finished! ", partition["name"])
		}

	}
	wg.Wait()

	log.Print("Finish...")

}

func Copy() {

	c := config.New()

	source := *database.New()
	source.SetDSN(c.SourceConnection)
	sourceTableSettings := source.CreateTableSettings(c.SourceConnection.Database, c.SourceConnection.Table)

	destination := *database.New()
	destination.SetDSN(c.DestinationConnection)
	destinationTableSettings := destination.CreateTableSettings(c.DestinationConnection.Database, c.DestinationConnection.Table)

	sourceTimezone, _ := source.GetTimezone()
	destinationTimezone, _ := destination.GetTimezone()

	if sourceTimezone != destinationTimezone {
		log.Fatal("different timezones!")
	} else {
		loc, _ := time.LoadLocation(sourceTimezone)
		time.Local = loc
		log.Debug("Timezone set to: " + sourceTimezone)
	}

	if sourceTableSettings.RowsCount == destinationTableSettings.RowsCount {
		log.Warn("[main] Row count in both tables same!")
		log.Warn("[main] Could be a problem")
	} else if sourceTableSettings.RowsCount < destinationTableSettings.RowsCount {
		log.Warn("[main] Row count in destination table more than in source table!")
		log.Warn("[main] Could be a problem")
	}

	var (
		sourceComapre              = make(chan uint64)
		destinationCompare         = make(chan uint64)
		sourcePartitionHash        uint64
		destinationPartitionHash   uint64
		partitionName              = []string{""}
		partitionKeysWithFunctions = false
		wg                         sync.WaitGroup
	)

	log.Debug("[main] Getting partition keys")
	sourcePartitionKeyStringArray := database.RegexPartitionKeysFromSettings(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyStringArrayRaw := database.RegexPartitionKeysFromSettingsRaw(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyMap := database.PartitionKeysToMap(sourcePartitionKeyStringArray, sourceTableSettings)
	log.Debug("[main] Done..")

	if len(sourcePartitionKeyStringArray) == 0 {
		log.Fatal("[main] No partition keys!")
	}

	values := database.ReturnValuesString(sourceTableSettings.Describe, destinationTableSettings.Describe)
	destinationValues := database.ReturnDestValues(sourceTableSettings.Describe, destinationTableSettings.Describe)

	for _, partition := range sourceTableSettings.TablePartitions {

		if len(sourcePartitionKeyStringArray) > 1 {
			partitionName = database.RegexPartitionName(partition["partition"])
		} else {
			partitionName[0] = fmt.Sprintf("%v", partition["partition"])
		}

		log.Printf("[%v] Generating Where", partition["partition"])
		if len(sourcePartitionKeyStringArrayRaw) == len(sourcePartitionKeyStringArray) {
			partitionKeysWithFunctions = true
		}
		currentWhere := database.GenerateWhere(partition["min_time"], partition["max_time"], sourcePartitionKeyMap, partitionName, sourcePartitionKeyStringArrayRaw, partitionKeysWithFunctions)

		if destination.CheckPartitionRowCount(currentWhere, destinationTableSettings) != 0 { //если  удаленная партиция пустая то и хэши сверять незачем

			wg.Add(2)

			if c.CheckHashes {

				go source.PartitionHashCheckAsync(sourceTableSettings, destinationValues, currentWhere, sourceComapre, &wg)
				go destination.PartitionHashCheckAsync(destinationTableSettings, values, currentWhere, destinationCompare, &wg)

			} else {

				go source.CheckPartitionRowCountAsync(currentWhere, sourceTableSettings, sourceComapre, &wg)
				go destination.CheckPartitionRowCountAsync(currentWhere, destinationTableSettings, destinationCompare, &wg)

			}

			sourcePartitionHash = <-sourceComapre
			destinationPartitionHash = <-destinationCompare

			log.Debug(sourcePartitionHash)
			log.Debug(destinationPartitionHash)

			if sourcePartitionHash == 0 && destinationPartitionHash == 0 {
				log.Fatal("hashes empty, looks like beda")
			}

			if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash == 0 {

				log.Printf("[%v] Destination partition empty! lets start copy partition", partition["name"])
				destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, destinationValues, currentWhere)
				log.Printf("[%v] Copy of partition is finished! ", partition["name"])
			} else if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash != 0 {
				log.Printf("[%v] Destination data inconsistent! remove needed", partition["name"])
				log.Printf("[%v] Removing...", partition["name"])
				destination.DeletePartition(destinationTableSettings, currentWhere)
				log.Printf("[%v] Remove finished", partition["name"])
				log.Printf("[%v] Starting to copy: ", partition["name"])
				destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, destinationValues, currentWhere)
				log.Printf("[%v] Finished", partition["name"])
			} else {
				log.Printf("[%v] Partitions identical ! skipping ", partition["name"])
			}

		} else {
			log.Printf("[%v] Destination partition row count is zero, so we just starting copy partition: ", partition["name"])
			destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, destinationValues, currentWhere)
			log.Printf("[%v] Copy of partition is finished! ", partition["name"])
		}

		wg.Wait()

	}

	log.Print("Finish...")
}
