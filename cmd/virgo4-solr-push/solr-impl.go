package main

import (
	"github.com/vanng822/go-solr/solr"
	"log"
)

// this is our implementation
type solrImpl struct {
	*solr.SolrInterface
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
	_, _, err := s.Ping()
	return err
}

func ( s * solrImpl ) Commit( ) error {
	//_, err := s.Commit()
	log.Printf("SOLR commit...")
	return nil
}

func ( s * solrImpl ) Add( string ) error {
	log.Printf("Send to SOLR...")
	return nil
}

//
// end of file
//