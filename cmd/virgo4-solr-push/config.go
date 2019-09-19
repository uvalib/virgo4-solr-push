package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName    string
	SolrUrl        string
	CoreName       string
	PollTimeOut    int64
	SolrBlockCount uint
	FlushTime      int
	CommitTime     int
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	log.Printf("Loading configuration...")
	var cfg ServiceConfig
	flag.StringVar(&cfg.InQueueName, "inqueue", "", "Inbound queue name")
	flag.StringVar(&cfg.SolrUrl, "solr", "", "SOLR endpoint")
	flag.StringVar(&cfg.CoreName, "core", "", "SOLR core name")
	flag.Int64Var(&cfg.PollTimeOut, "pollwait", 15, "Poll wait time (in seconds)")
	flag.UintVar(&cfg.SolrBlockCount, "blockcount", 250, "SOLR send block size")
	flag.IntVar(&cfg.FlushTime, "flushtime", 30, "Flush time (in seconds)")
	flag.IntVar(&cfg.CommitTime, "committime", 180, "Commit time (in seconds)")

	flag.Parse()

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName )
	log.Printf("[CONFIG] SolrUrl              = [%s]", cfg.SolrUrl )
	log.Printf("[CONFIG] CoreName             = [%s]", cfg.CoreName )
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut )
	log.Printf("[CONFIG] SolrBlockCount       = [%d]", cfg.SolrBlockCount)
	log.Printf("[CONFIG] FlushTime            = [%d]", cfg.FlushTime )
	log.Printf("[CONFIG] CommitTime           = [%d]", cfg.CommitTime )

	return &cfg
}

//
// end of file
//