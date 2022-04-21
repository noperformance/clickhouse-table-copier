package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"sync"
)

type Connection struct {
	HostName    string `yaml:"host"`
	UserName    string `yaml:"user,omitempty"`
	Password    string `yaml:"password,omitempty"`
	Port        uint16 `yaml:"port,omitempty"`
	KeyFilename string `yaml:"key_filename,omitempty"`
	Secure      bool   `yaml:"secure,omitempty"`
	SkipVerify  bool   `yaml:"skip_verify,omitempty"`
	Database    string `yaml:"database"`
	Table       string `yaml:"table"`
}

type WorkerPoolT struct {
	NumWorkers int `yaml:"num_workers"`
	NumRetry   int `yaml:"num_retry"`
	ChanLen    int `yaml:"chan_len"`
}

type config struct {
	SourceConnection      Connection  `yaml:"source"`
	DestinationConnection Connection  `yaml:"destination"`
	WorkerPool            WorkerPoolT `yaml:"worker_pool"`
	Debug                 bool        `yaml:"debug"`
	CheckHashes           bool        `yaml:"check_hashes"`
}

var (
	once     sync.Once
	instance *config
)

func New() *config {
	once.Do(func() {
		instance = new(config)
	})
	return instance
}

func (c *config) Read(filename string) error {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
		return err
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Printf("Unmarshal: %v", err)
		return err
	}
	return nil
}

func (c *config) Check() bool {
	if c.DestinationConnection.Database == "" || c.DestinationConnection.Table == "" || c.SourceConnection.Database == "" || c.SourceConnection.Table == "" || c.DestinationConnection.HostName == "" || c.SourceConnection.HostName == "" {
		return false
	}
	return true
}

func (c *config) Print() {
	fmt.Println(c)
}
