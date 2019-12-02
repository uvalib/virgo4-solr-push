package main

type SOLR interface {
	BufferDoc([]byte) error    // add a document to the buffer in preparation to send to SOLR
	IsAlive() error            // is our endpoint alive?
	IsTimeToAdd() bool         // is it time to add our pending documents
	IsTimeToCommit() bool      // is our endpoint alive?
	ForceAdd() (string, error) // force an add for pending documents (returns document number of failing item)
	ForceCommit() error        // force a commit
}

// Initialize our SOLR connection
func NewSolr(id int, config ServiceConfig) (SOLR, error) {

	// mock implementation here if necessary
	return newSolr(id, config)
}

//
// end of file
//
