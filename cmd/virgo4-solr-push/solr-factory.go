package main

import (
	//"fmt"
	"fmt"
	"net/http"
	"time"
)

// this is our actual implementation
type solrImpl struct {
	Config     ServiceConfig // our original configuration object
	CommsDebug bool          // debugging of communication with SOLR
	PostUrl    string        // the actual URL to Add/Commit too
	PingUrl    string        // the actual URL to Ping

	// internal state stuff
	lastCommit     time.Time // when we did our last commit to SOLR
	lastAdd        time.Time // when we did our last add to SOLR
	solrDirty      bool      // we have added documents to SOLR without committing
	pendingAdds    uint      // how many documents in the add buffer
	pendingAddIds  []string  // our document add buffer
	addBuffer      []byte    // our document add buffer
	sendBufferSize uint      // the default document add buffer size

	workerId int // used for logging

	httpClient *http.Client // our http client connection
}

// Initialize our SOLR implementation
func newSolr(id int, config ServiceConfig) (SOLR, error) {

	impl := &solrImpl{Config: config, workerId: id}
	impl.PostUrl = fmt.Sprintf("%s/%s/update", config.SolrUrl, config.SolrCoreName)
	impl.PingUrl = fmt.Sprintf("%s/%s/admin/ping", config.SolrUrl, config.SolrCoreName)

	// cos zero values are not correct
	impl.lastCommit = time.Now()
	impl.lastAdd = time.Now()

	// turn into megabytes and allocate the send buffer
	impl.sendBufferSize = 1024 * 1024 * config.SolrBufferSize
	impl.addBuffer = make([]byte, 0, impl.sendBufferSize)

	// configure the client
	impl.httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 5,
		},
		Timeout: time.Duration(config.SolrTimeout) * time.Second,
	}

	return impl, impl.IsAlive()
}

//
// end of file
//
