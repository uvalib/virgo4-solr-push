package main

import "time"

type SOLR interface {
	BufferDoc([]byte) error  // add a document to the buffer in preparation to send to SOLR
	IsAlive() error          // is our endpoint alive?
	IsTimeToAdd() bool       // is it time to add our pending documents
	IsTimeToCommit() bool    // is our endpoint alive?
	ForceAdd() (uint, error) // force an add for pending documents (returns index of the first document to fail)
	ForceCommit() error      // force a commit
}

type SolrConfig struct {
	EndpointUrl    string        // the endpoint URL for the SOLR instance
	CoreName       string        // the core name
	MaxBlockCount  uint          // the maximum number of items to accumulate in a block before adding
	CommitTime     time.Duration // the maximum time to wait between SOLR commits (in seconds)
	FlushTime      time.Duration // the maximum time to wait between SOLR block adds (in seconds)
	RequestTimeout time.Duration // the maximum time to wait before a request times out (in seconds)
}

// Initialize our SOLR connection
func NewSolr(id int, config SolrConfig) (SOLR, error) {

	// mock implementation here if necessary
	return newSolr(id, config)
}

//
// end of file
//
