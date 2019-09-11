package main

//import (
//)

type SOLR interface {
	Check( ) error
	Commit( ) error
	Add( string ) error
}

// our singleton instance
var Solr SOLR

// Initialize our SOLR connection
func NewSolr( solrUrl string, core string ) error {

	var err error
	// mock implementation here if necessary
	Solr, err = newSolr( solrUrl, core )
	return err
}

//
// end of file
//