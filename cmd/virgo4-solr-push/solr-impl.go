package main

import (
	"github.com/vanng822/go-solr/solr"
	//"log"
)

// this is our implementation
type solrImpl struct {
	si * solr.SolrInterface
}

// Initialize our SOLR connection
func newSolr( solrUrl string, core string ) ( SOLR, error ) {

	si, err := solr.NewSolrInterface( solrUrl, core )
	if err != nil {
		return nil, err
	}

	if _, _, err = si.Ping(); err != nil {
		return nil, err
	}

	return &solrImpl{ si }, nil
}

func ( s * solrImpl ) Check( ) error {
	_, _, err := s.si.Ping()
	return err
}

func ( s * solrImpl ) Commit( ) error {
	_, err := s.si.Commit()
	//log.Printf("SOLR commit...")
	return err
}

func ( s * solrImpl ) Add( data string ) error {

	_, err := s.si.Update( data, nil )

	//log.Printf("SOLR: success %t", resp.Success )
	//log.Printf("SOLR: result  %t", resp.Result )
	//if err != nil {
	//	log.Printf("SOLR: error  %t", err )
	//}
	return err
}

//
// end of file
//