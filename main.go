package main

import (
	"clickhouse-table-copier/config"
	"clickhouse-table-copier/copy"
	"clickhouse-table-copier/status"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"os"
	"runtime"
	//"github.com/urfave/cli/v2"
)

type MainArgs struct {
	configFile string
	debug      bool
	version    bool
	infoMode   bool
	copyMode   bool
	asyncMode  bool
}

func (ma *MainArgs) parseMode() error {
	modeCount := 0
	if ma.copyMode {
		modeCount++
	}
	if ma.infoMode {
		modeCount++
	}
	if ma.version {
		modeCount++
	}
	if ma.asyncMode {
		modeCount++
	}
	if modeCount == 1 {
		return nil
	}
	return errors.New("bad command line args usage: sync/info")
}

func main() {

	runtime.GOMAXPROCS(4)

	var cargs MainArgs

	flag.BoolVarP(&cargs.version, "version", "v", false, "Get version")
	flag.BoolVarP(&cargs.debug, "debug", "d", false, "Enable debug")
	flag.BoolVarP(&cargs.infoMode, "info", "i", false, "Enable information mode")
	flag.BoolVarP(&cargs.copyMode, "sync", "s", false, "Enable copymode")
	flag.StringVarP(&cargs.configFile, "config", "c", "config.yaml", "Path to config file")
	flag.Parse()

	err := cargs.parseMode()
	if err != nil {
		log.Print(err)
		flag.Usage()
		log.Fatalf("Exit by error on parse cmd args")
	}

	if cargs.version {
		fmt.Println(clickTableCopier.GetVersion())
		os.Exit(0)
	}

	s := status.New()
	c := config.New()
	err = c.Read(cargs.configFile)
	if err != nil {
		s.SetStatus(status.FailReadConfig)
		println(err)
		flag.Usage()
		log.Println("Please check config file")
		os.Exit(s.GetFinalStatus())
	}

	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	if c.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if !c.Check() {
		s.SetStatus(status.FailCheck)
		log.Fatal("Check configuration, db/table/host is necessary")
	}

	if cargs.infoMode {
		copy.Info()
	} else if cargs.copyMode {
		copy.Copy()
	} else {
		log.Fatal("?")
	}

	if err != nil {
		log.Println(err)
	}
	log.Printf("Exit %d", s.GetFinalStatus())
	os.Exit(s.GetFinalStatus())

}
