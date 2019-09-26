package main

import (
   //"fmt"
   "fmt"
   "time"
)

// this is our actual implementation
type solrImpl struct {

   Config      SolrConfig        // our original configuration object
   CommsDebug  bool              // debugging of communication with SOLR
   PostUrl     string            // the actual URL to Add/Commit too
   PingUrl     string            // the actual URL to Ping

   // internal state stuff
   lastCommit  time.Time         // when we did our last commit to SOLR
   lastAdd     time.Time         // when we did our last add to SOLR
   solrDirty   bool              // we have added documents to SOLR without committing
   pendingAdds uint              // how many documents in the add buffer
   addBuffer   []byte            // our document add buffer

   workerId    int               // used for logging
}

// Initialize our SOLR implementation
func newSolr( id int, config SolrConfig ) ( SOLR, error ) {

   impl := &solrImpl{ Config: config, workerId: id }
   impl.PostUrl = fmt.Sprintf( "%s/%s/update", config.EndpointUrl, config.CoreName )
   impl.PingUrl = fmt.Sprintf( "%s/%s/admin/ping", config.EndpointUrl, config.CoreName )

   // cos zero values are not correct
   impl.lastCommit = time.Now( )
   impl.lastAdd = time.Now( )

   impl.addBuffer = make( []byte, 0, 1024 * 1024 )

   return impl, impl.IsAlive( )
}

//
// end of file
//
