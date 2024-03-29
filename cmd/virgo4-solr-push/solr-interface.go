package main

// SOLR - our SOLR interface
type SOLR interface {
	BufferDoc(string, []byte) error // add a document to the buffer in preparation to send to SOLR
	IsAlive() error                 // is our endpoint alive?
	IsTimeToAdd() bool              // is it time to add our pending documents
	IsTimeToCommit() bool           // is it time to commit?
	ForceAdd() (string, error)      // force an add for pending documents (returns document number of any failing item)
	ForceCommit() error             // force a commit
}

// NewSolr - Initialize our SOLR connection
func NewSolr(id int, config ServiceConfig) (SOLR, error) {

	// mock implementation here if necessary
	return newSolr(id, config)
}

//
// end of file
//
