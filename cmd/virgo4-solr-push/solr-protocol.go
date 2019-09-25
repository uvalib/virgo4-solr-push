package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"

	//"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/parnurzeal/gorequest"
	"github.com/antchfx/xmlquery"
)

var documentAddFailed = fmt.Errorf( "SOLR add failed" )

func ( s * solrImpl ) protocolCommit( ) error {

	body, err := s.httpPost( "<commit/>" )
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

func ( s * solrImpl ) protocolAdd( builder strings.Builder ) error {

	body, err := s.httpPost( builder.String( ) )
	if err != nil {
		return err
	}

	_, docix, err := s.processResponsePayload( body )
	if err != nil {
		if err == documentAddFailed {

			// special case here...
			log.Printf("ERROR: adds after document %d FAILED (ignoring for now)", docix )

			return nil
		}

		return err
	}

	return nil
}

func ( s * solrImpl ) httpPost( payload string ) ( string, error ){

	//fmt.Printf( "%s\n", s.url )

	resp, body, errs := gorequest.New().
		SetDebug( s.CommsDebug ).
		Post( s.PostUrl ).
		Send( payload ).
		Timeout( s.Config.RequestTimeout ).
		Set( "Content-Type", "application/xml" ).
		Set( "Accept", "application/json" ).
		End()

	if errs != nil {
		return "", errs[ 0 ]
	}

	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()

	//log.Printf( body )
    return body, nil
}

func ( s * solrImpl ) processResponsePayload( body string ) ( int, int, error ) {

	// generate a query structure from the body
	doc, err := xmlquery.Parse( strings.NewReader( body ) )
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
				docix, _ := strconv.Atoi( match[ 1 ] )
				return status, docix, documentAddFailed
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