package main

import (
	"log"
	"os"
	"strconv"
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

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt( env string ) int {

	number := ensureSetAndNonEmpty( env )
	n, err := strconv.Atoi( number )
	if err != nil {

		os.Exit(1)
	}
	return n
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	cfg.InQueueName = ensureSetAndNonEmpty( "VIRGO4_SOLR_PUSH_IN_QUEUE" )
	cfg.SolrUrl = ensureSetAndNonEmpty( "VIRGO4_SOLR_PUSH_SOLR_URL" )
	cfg.CoreName = ensureSetAndNonEmpty( "VIRGO4_SOLR_PUSH_SOLR_CORE" )
	cfg.PollTimeOut = int64( envToInt( "VIRGO4_SOLR_PUSH_QUEUE_POLL_TIMEOUT" ) )
	cfg.SolrBlockCount = uint( envToInt( "VIRGO4_SOLR_PUSH_SOLR_BLOCK_COUNT" ) )
	cfg.FlushTime = envToInt( "VIRGO4_SOLR_PUSH_SOLR_FLUSH_TIME" )
	cfg.CommitTime = envToInt( "VIRGO4_SOLR_PUSH_SOLR_COMMIT_TIME" )

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