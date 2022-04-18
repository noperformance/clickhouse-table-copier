package copy

import (
	"clickhouse-table-copier/config"
	"clickhouse-table-copier/database"
	"fmt"
	log "github.com/sirupsen/logrus"
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
	} else if sourceTableSettings.RowsCount < destinationTableSettings.RowsCount {
		log.Warn("Row count in destination table more than in source table!")
	}

	var (
		sourcePartitionHash        uint64
		destinationPartitionHash   uint64
		partitionName              = []string{""}
		partitionKeysWithFunctions = false
	)

	log.Debug("Geting partition keys")
	sourcePartitionKeyStringArray := source.RegexPartitionKeysFromSettings(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyStringArrayRaw := source.RegexPartitionKeysFromSettingsRaw(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyMap := source.PartitionKeysToMap(sourcePartitionKeyStringArray, sourceTableSettings)
	log.Debug("Done..")

	if len(sourcePartitionKeyStringArray) == 0 {
		log.Fatal("No partition keys!")
	}

	for _, partition := range sourceTableSettings.TablePartitions {

		log.Info("Preparing to copy partition: ", partition["name"])

		if len(sourcePartitionKeyStringArray) > 1 {
			partitionName = source.RegexPartitionName(partition["partition"])
		} else {
			partitionName[0] = fmt.Sprintf("%v", partition["partition"])
		}

		log.Debug("Generating Where")
		if len(sourcePartitionKeyStringArrayRaw) == len(sourcePartitionKeyStringArray) {
			partitionKeysWithFunctions = true
		}
		currentWhere := source.GenerateWhere(partition["min_time"], partition["max_time"], sourcePartitionKeyMap, partitionName, sourcePartitionKeyStringArrayRaw, partitionKeysWithFunctions)
		log.Debug("Done..")

		sourcePartitionHash = source.PartitionHashCheck(sourceTableSettings, source.ReturnValuesString(sourceTableSettings.Describe), currentWhere)
		destinationPartitionHash = destination.PartitionHashCheck(destinationTableSettings, source.ReturnValuesString(sourceTableSettings.Describe), currentWhere)

		if sourcePartitionHash == 0 && destinationPartitionHash == 0 {
			log.Fatal("hashes empty, looks like beda")
		}

		if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash == 0 {
			log.Print("Destination partition empty! lets start copy partition: ", partition["name"])

		} else if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash != 0 {
			log.Print("Destination data unconsistent! remove needed")
			log.Print("Removing...")

		} else {
			log.Print("Partitions identical ! skiping ")
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
	} else if sourceTableSettings.RowsCount < destinationTableSettings.RowsCount {
		log.Warn("Row count in destination table more than in source table!")
	}

	var (
		sourcePartitionHash        uint64
		destinationPartitionHash   uint64
		partitionName              = []string{""}
		partitionKeysWithFunctions = false
	)

	log.Debug("Geting partition keys")
	sourcePartitionKeyStringArray := source.RegexPartitionKeysFromSettings(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyStringArrayRaw := source.RegexPartitionKeysFromSettingsRaw(sourceTableSettings.Info[0]["partition_key"], sourceTableSettings)
	sourcePartitionKeyMap := source.PartitionKeysToMap(sourcePartitionKeyStringArray, sourceTableSettings)
	log.Debug("Done..")

	if len(sourcePartitionKeyStringArray) == 0 {
		log.Fatal("No partition keys!")
	}

	for _, partition := range sourceTableSettings.TablePartitions {

		log.Info("Preparing to copy partition: ", partition["name"])

		if len(sourcePartitionKeyStringArray) > 1 {
			partitionName = source.RegexPartitionName(partition["partition"])
		} else {
			partitionName[0] = fmt.Sprintf("%v", partition["partition"])
		}

		log.Debug("Generating Where")
		if len(sourcePartitionKeyStringArrayRaw) == len(sourcePartitionKeyStringArray) {
			partitionKeysWithFunctions = true
		}
		currentWhere := source.GenerateWhere(partition["min_time"], partition["max_time"], sourcePartitionKeyMap, partitionName, sourcePartitionKeyStringArrayRaw, partitionKeysWithFunctions)
		log.Debug("Done..")

		sourcePartitionHash = source.PartitionHashCheck(sourceTableSettings, source.ReturnValuesString(sourceTableSettings.Describe), currentWhere)
		destinationPartitionHash = destination.PartitionHashCheck(destinationTableSettings, source.ReturnValuesString(sourceTableSettings.Describe), currentWhere)

		if sourcePartitionHash == 0 && destinationPartitionHash == 0 {
			log.Fatal("hashes empty, looks like beda")
		}

		if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash == 0 {
			log.Print("Destination partition empty! lets start copy partition: ", partition["name"])

			destination.CopyPartition(sourceTableSettings, destinationTableSettings, c.SourceConnection, source.ReturnValuesString(sourceTableSettings.Describe), currentWhere)

			destinationPartitionHash = destination.PartitionHashCheck(destinationTableSettings, source.ReturnValuesString(sourceTableSettings.Describe), currentWhere)
			if sourcePartitionHash == destinationPartitionHash {
				log.Print("Sync complete successfully")
			} else {
				log.Fatal("Hashes didnt match after sync, beda")
			}

			log.Printf("Copy of partition: %v is finished! ", partition["name"])

		} else if sourcePartitionHash != destinationPartitionHash && destinationPartitionHash != 0 {
			log.Print("Destination data unconsistent! remove needed")
			log.Print("Removing...")

			destination.DeletePartition(destinationTableSettings, currentWhere)

		} else {
			log.Print("Partitions identical ! skiping ")
		}

	}

	log.Print("Finish...")

}
