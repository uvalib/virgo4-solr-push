package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"regexp"
	"strconv"

	"github.com/antchfx/xmlquery"
)

var documentAddFailed = fmt.Errorf( "SOLR add failed" )

func ( s * solrImpl ) protocolCommit( ) error {

	body, err := s.httpPost( []byte( "<commit/>" ) )
	if err != nil {
	   return err
	}

	_, _, err = s.processResponsePayload( body )
	if err != nil {
		return err
	}

	//log.Printf("SOLR commit...")
	return nil
}

func ( s * solrImpl ) protocolAdd( buffer []byte ) ( uint, error ) {

	body, err := s.httpPost( buffer )
	if err != nil {
		return 0, err
	}

	_, docIx, err := s.processResponsePayload( body )
	if err != nil {

		// one of the documents in the add list failed
		if err == documentAddFailed {
			// special case here...
			log.Printf("ERROR: add document at index %d FAILED", docIx )
			return docIx, err
		}

		return 0, err
	}

	// all good
	return 0, nil
}

func ( s * solrImpl ) httpPost( buffer []byte ) ( []byte, error ){

	//fmt.Printf( "%s\n", s.url )

	req, err := http.NewRequest("POST", s.PostUrl, bytes.NewBuffer( buffer ) )
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/xml" )
	//req.Header.Set("Accept", "application/json" )

	response, err := s.httpClient.Do( req )
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll( response.Body )
	if err != nil {
		return nil, err
	}

	//log.Printf( body )
	return body, nil
}

func ( s * solrImpl ) processResponsePayload( body []byte ) ( int, uint, error ) {

	// generate a query structure from the body
	doc, err := xmlquery.Parse( bytes.NewReader( body ) )
	if err != nil {
		return 0, 0, err
	}

	// attempt to extract the statusNode field
	statusNode := xmlquery.FindOne( doc, "//response/lst[@name='responseHeader']/int[@name='status']")
	if statusNode == nil {
		return 0, 0, fmt.Errorf( "Cannot find status field in response payload (%s)", body )
	}

	// if it appears that we have an error
	if statusNode.InnerText( ) != "0" {

		// extract the status and attempt to find the error messageNode body
		status, _ := strconv.Atoi( statusNode.InnerText( ) )

		messageNode := xmlquery.FindOne( doc, "//response/lst[@name='error']/str[@name='msg']")
		if messageNode != nil {

			// if this is an error on a specific document, we can extract that information
			re := regexp.MustCompile(`\[(\d+),\d+\]`)
			match := re.FindStringSubmatch( messageNode.InnerText( ) )
			if match != nil {
				fmt.Printf( "%s", body )
				docnum, _ := strconv.Atoi( match[ 1 ] )

				// return index of failing item
				return status, uint( docnum ) - 1, documentAddFailed
			}
		}
		return status, 0, fmt.Errorf( "%s", body )
	}

	// all good
    return 0, 0, nil
}

//
// end of file
//