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
		log.Warn("Row count in both tables same!")
		log.Fatal("Could be a problem")
	} else if sourceTableSettings.RowsCount < destinationTableSettings.RowsCount {
		log.Warn("Row count in destination table more than in source table!")
		log.Fatal("Could be a problem")
	}

	var (
		sourcePartitionHash        uint64
		destinationPartitionHash   uint64
		partitionName              = []string{""}
		partitionKeysWithFunctions = false
		values                     = ""
	)

	log.Debug("Getting partition keys")
	sourcePartitionKeyStringArray := database.RegexPartitionKeysFromSettings(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyStringArrayRaw := database.RegexPartitionKeysFromSettingsRaw(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyMap := database.PartitionKeysToMap(sourcePartitionKeyStringArray, sourceTableSettings)
	log.Debug("Done..")

	if len(sourcePartitionKeyStringArray) == 0 {
		log.Fatal("No partition keys!")
	}

	if len(database.ReturnValuesString(sourceTableSettings.Describe)) < len(database.ReturnValuesString(destinationTableSettings.Describe)) {
		values = database.ReturnValuesString(sourceTableSettings.Describe)
	} else {
		values = database.ReturnValuesString(destinationTableSettings.Describe)
	}

	for _, partition := range sourceTableSettings.TablePartitions {

		log.Info("Preparing to copy partition: ", partition["name"])

		if len(sourcePartitionKeyStringArray) > 1 {
			partitionName = database.RegexPartitionName(partition["partition"])
		} else {
			partitionName[0] = fmt.Sprintf("%v", partition["partition"])
		}

		log.Print("Generating Where")
		if len(sourcePartitionKeyStringArrayRaw) == len(sourcePartitionKeyStringArray) {
			partitionKeysWithFunctions = true
		}
		currentWhere := database.GenerateWhere(partition["min_time"], partition["max_time"], sourcePartitionKeyMap, partitionName, sourcePartitionKeyStringArrayRaw, partitionKeysWithFunctions)

		log.Debug(currentWhere)

		if destination.CheckPartitionRowCount(currentWhere, destinationTableSettings) != 0 { //если  удаленная партиция пустая то и хэши сверять незачем

			sourcePartitionHash = source.PartitionHashCheck(sourceTableSettings, values, currentWhere)
			destinationPartitionHash = destination.PartitionHashCheck(destinationTableSettings, values, currentWhere)

			if sourcePartitionHash == 0 && destinationPartitionHash == 0 {
				log.Fatal("hashes empty, looks like beda")
			}

			if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash == 0 {
				log.Print("Destination partition empty! lets start copy partition: ", partition["name"])
				//destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, currentWhere)
				log.Printf("Copy of partition: %v is finished! ", partition["name"])
			} else if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash != 0 {
				log.Print("Destination data inconsistent! remove needed")
				log.Print("Removing...")
				//destination.DeletePartition(destinationTableSettings, currentWhere)
				log.Print("Remove finished")
			} else {
				log.Print("Partitions identical ! skipping ")
			}
		} else {
			log.Print("Destination partition row count is zero, so we just starting copy partition: ", partition["name"])
			//destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, currentWhere)
			log.Printf("Copy of partition: %v is finished! ", partition["name"])
		}
	}

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
		log.Warn("Row count in both tables same!")
		log.Fatal("Could be a problem")
	} else if sourceTableSettings.RowsCount < destinationTableSettings.RowsCount {
		log.Warn("Row count in destination table more than in source table!")
		log.Fatal("Could be a problem")
	}

	var (
		sourcePartitionHash        uint64
		destinationPartitionHash   uint64
		partitionName              = []string{""}
		partitionKeysWithFunctions = false
		values                     = ""
	)

	log.Debug("Getting partition keys")
	sourcePartitionKeyStringArray := database.RegexPartitionKeysFromSettings(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyStringArrayRaw := database.RegexPartitionKeysFromSettingsRaw(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyMap := database.PartitionKeysToMap(sourcePartitionKeyStringArray, sourceTableSettings)
	log.Debug("Done..")

	if len(sourcePartitionKeyStringArray) == 0 {
		log.Fatal("No partition keys!")
	}

	if len(database.ReturnValuesString(sourceTableSettings.Describe)) < len(database.ReturnValuesString(destinationTableSettings.Describe)) {
		values = database.ReturnValuesString(sourceTableSettings.Describe)
	} else {
		values = database.ReturnValuesString(destinationTableSettings.Describe)
	}

	for _, partition := range sourceTableSettings.TablePartitions {

		log.Info("Preparing to copy partition: ", partition["name"])

		if len(sourcePartitionKeyStringArray) > 1 {
			partitionName = database.RegexPartitionName(partition["partition"])
		} else {
			partitionName[0] = fmt.Sprintf("%v", partition["partition"])
		}

		log.Print("Generating Where")
		if len(sourcePartitionKeyStringArrayRaw) == len(sourcePartitionKeyStringArray) {
			partitionKeysWithFunctions = true
		}
		currentWhere := database.GenerateWhere(partition["min_time"], partition["max_time"], sourcePartitionKeyMap, partitionName, sourcePartitionKeyStringArrayRaw, partitionKeysWithFunctions)

		log.Debug(currentWhere)

		if destination.CheckPartitionRowCount(currentWhere, destinationTableSettings) != 0 { //если  удаленная партиция пустая то и хэши сверять незачем

			sourcePartitionHash = source.PartitionHashCheck(sourceTableSettings, values, currentWhere)
			destinationPartitionHash = destination.PartitionHashCheck(destinationTableSettings, values, currentWhere)

			if sourcePartitionHash == 0 && destinationPartitionHash == 0 {
				log.Fatal("hashes empty, looks like beda")
			}

			if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash == 0 {
				log.Print("Destination partition empty! lets start copy partition: ", partition["name"])
				destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, currentWhere)
				log.Printf("Copy of partition: %v is finished! ", partition["name"])
			} else if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash != 0 {
				log.Print("Destination data inconsistent! remove needed")
				log.Print("Removing...")
				destination.DeletePartition(destinationTableSettings, currentWhere)
				log.Print("Remove finished")
			} else {
				log.Print("Partitions identical ! skipping ")
			}
		} else {
			log.Print("Destination partition row count is zero, so we just starting copy partition: ", partition["name"])
			destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, currentWhere)
			log.Printf("Copy of partition: %v is finished! ", partition["name"])
		}
	}

	log.Print("Finish...")

}

func AsyncCopy() {

	log.Print("--async--")

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
		sourceHashChan             = make(chan uint64)
		destinationHashChan        = make(chan uint64)
		sourcePartitionHash        uint64
		destinationPartitionHash   uint64
		partitionName              = []string{""}
		partitionKeysWithFunctions = false
		values                     = ""
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

	if len(database.ReturnValuesString(sourceTableSettings.Describe)) < len(database.ReturnValuesString(destinationTableSettings.Describe)) {
		values = database.ReturnValuesString(sourceTableSettings.Describe)
	} else {
		values = database.ReturnValuesString(destinationTableSettings.Describe)
	}

	for _, partition := range sourceTableSettings.TablePartitions {

		wg.Add(1)

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

			go source.PartitionHashCheckAsync(sourceTableSettings, values, currentWhere, sourceHashChan, &wg)
			go destination.PartitionHashCheckAsync(destinationTableSettings, values, currentWhere, destinationHashChan, &wg)

			sourcePartitionHash = <-sourceHashChan
			destinationPartitionHash = <-destinationHashChan

			if sourcePartitionHash == 0 && destinationPartitionHash == 0 {
				log.Fatal("hashes empty, looks like beda")
			}

			if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash == 0 {
				log.Printf("[%v] Destination partition empty! lets start copy partition", partition["name"])
				go destination.CopyPartitionAsync(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, currentWhere, &wg)
				log.Printf("[%v] Copy of partition is finished! ", partition["name"])
			} else if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash != 0 {
				//wg.Add(1)
				log.Printf("[%v] Destination data inconsistent! remove needed", partition["name"])
				log.Printf("[%v] Removing...", partition["name"])
				//go destination.DeletePartitionAsync(destinationTableSettings, currentWhere, &wg)
				log.Printf("[%v] Remove finished", partition["name"])
				log.Printf("[%v] Starting to copy: ", partition["name"])
				go destination.CopyPartitionAsync(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, currentWhere, &wg)
				log.Printf("[%v] Finished", partition["name"])
			} else {
				wg.Add(1)
				log.Printf("[%v] Partitions identical ! skipping ", partition["name"])
			}

		} else {
			log.Printf("[%v] Destination partition row count is zero, so we just starting copy partition: ", partition["name"])
			go destination.CopyPartitionAsync(sourceTableSettings, destinationTableSettings, c.SourceConnection, values, currentWhere, &wg)
			log.Printf("[%v] Copy of partition is finished! ", partition["name"])
		}

	}
	wg.Wait()

	log.Print("Finish...")

	//
	//if <-sourcePartitionHash == 0 && <-destinationPartitionHash == 0 {

}
