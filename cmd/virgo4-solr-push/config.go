package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName     string // SQS queue name for inbound documents
	PollTimeOut     int64  // the SQS queue timeout (in seconds)

	SolrUrl        string  // the SOLR endpoint URL
	SolrCoreName   string  // the SOLR core name
	SolrTimeout    int     // the http timeout (in seconds)
	SolrBlockCount uint    // the maximum number of Solr AddDocs in a buffer sent to SOLR
	SolrFlushTime  int     // how often to flush the AddDocs buffer
	SolrCommitTime int     // how often to do a SOLR commit if dirty (in seconds)

	WorkerQueueSize int    // the inbound message queue size to feed the workers
	Workers         int    // the number of worker processes
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
	cfg.PollTimeOut = int64( envToInt( "VIRGO4_SOLR_PUSH_QUEUE_POLL_TIMEOUT" ) )

	cfg.SolrUrl = ensureSetAndNonEmpty( "VIRGO4_SOLR_PUSH_SOLR_URL" )
	cfg.SolrCoreName = ensureSetAndNonEmpty( "VIRGO4_SOLR_PUSH_SOLR_CORE" )
	cfg.SolrTimeout = envToInt( "VIRGO4_SOLR_PUSH_SOLR_TIMEOUT" )
	cfg.SolrBlockCount = uint( envToInt( "VIRGO4_SOLR_PUSH_SOLR_BLOCK_COUNT" ) )
	cfg.SolrFlushTime = envToInt( "VIRGO4_SOLR_PUSH_SOLR_FLUSH_TIME" )
	cfg.SolrCommitTime = envToInt( "VIRGO4_SOLR_PUSH_SOLR_COMMIT_TIME" )
	cfg.WorkerQueueSize = envToInt( "VIRGO4_SOLR_PUSH_WORK_QUEUE_SIZE" )
	cfg.Workers = envToInt( "VIRGO4_SOLR_PUSH_WORKERS" )

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName )
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut )

	log.Printf("[CONFIG] SolrUrl              = [%s]", cfg.SolrUrl )
	log.Printf("[CONFIG] SolrCoreName         = [%s]", cfg.SolrCoreName)
	log.Printf("[CONFIG] SolrTimeout          = [%d]", cfg.SolrTimeout)
	log.Printf("[CONFIG] SolrBlockCount       = [%d]", cfg.SolrBlockCount)
	log.Printf("[CONFIG] SolrFlushTime        = [%d]", cfg.SolrFlushTime)
	log.Printf("[CONFIG] SolrCommitTime       = [%d]", cfg.SolrCommitTime)
	log.Printf("[CONFIG] WorkerQueueSize      = [%d]", cfg.WorkerQueueSize )
	log.Printf("[CONFIG] Workers              = [%d]", cfg.Workers )

	return &cfg
}

//
// end of file
//