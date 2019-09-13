package main

import (
	//"errors"
	//"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	//"net/http"
	"time"

	"github.com/parnurzeal/gorequest"
	"github.com/antchfx/xmlquery"

	//"log"
)

// this is our implementation
type solrImpl struct {
	url       string
	httpDebug bool
}

// Initialize our SOLR connection
func newSolr( solrUrl string, core string ) ( SOLR, error ) {

	return &solrImpl{ url: fmt.Sprintf( "%s/%s/update", solrUrl, core ) }, nil
}

func ( s * solrImpl ) Check( ) error {
	//_, _, err := s.si.Ping()
	return nil
}

func ( s * solrImpl ) Commit( ) error {

	body, errs := s.httpPost( "<commit/>" )

	if errs != nil {
	   log.Fatal( errs )
	}

	// do stuff with body
	doc, err := xmlquery.Parse( strings.NewReader( body ) )
	if err != nil {
		log.Fatal( err )
	}

	status := xmlquery.FindOne( doc, "//response/lst[@name='responseHeader']/int[@name='status']")
	if status == nil {
		log.Fatal( "Cannot find status field in response payload (%s)", body )
	}

	if status.InnerText( ) != "0" {
		log.Printf( body )
		return fmt.Errorf( "SOLR response status = %s", status.InnerText( ) )
	}

	//log.Printf("SOLR commit...")
	return nil
}

func ( s * solrImpl ) Add( data string ) error {

	body, errs := s.httpPost( data )

	if errs != nil {
		log.Fatal( errs )
	}

	// do stuff with body

	doc, err := xmlquery.Parse( strings.NewReader( body ) )
	if err != nil {
		log.Fatal( err )
	}

	status := xmlquery.FindOne( doc, "//response/lst[@name='responseHeader']/int[@name='status']")
    if status == nil {
		log.Fatal( "Cannot find status field in response payload (%s)", body )
	}

	if status.InnerText( ) != "0" {
		log.Printf( body )
		return fmt.Errorf( "SOLR response status = %s", status.InnerText( ) )
	}

	return nil
}

func ( s * solrImpl ) httpPost( payload string ) ( string, []error ){

	//fmt.Printf( "%s\n", s.url )

	resp, body, errs := gorequest.New().
		SetDebug( s.httpDebug ).
		Post( s.url ).
		Send( payload ).
		Timeout(time.Duration( 600 ) * time.Second).
		Set( "Content-Type", "application/xml" ).
		Set( "Accept", "application/json" ).
		End()

	if errs != nil {
		return "", errs
	}

	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()

	//log.Printf( body )
    return body, nil
}

//
// end of file
//