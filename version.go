package main

import log "github.com/sirupsen/logrus"

var (
	clickTableCopier = clickTableCopierVersion{
		progName: "clickhouse-table-copier",
		majorVer: 0,
		minorVer: 4,
	}
	BuildVer = ""
)

type clickTableCopierVersion struct {
	majorVer int
	minorVer int
	progName string
}

func (c *clickTableCopierVersion) GetVersion() bool {
	log.Printf("%s version %d.%d %s", c.progName, c.majorVer, c.minorVer, BuildVer)
	return true
}
