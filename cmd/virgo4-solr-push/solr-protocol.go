package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"github.com/parnurzeal/gorequest"
	"github.com/antchfx/xmlquery"
)

func ( s * solrImpl ) protocolCommit( ) error {

	body, err := s.httpPost( "<commit/>" )

	if err != nil {
	   return err
	}

	// do stuff with body
	doc, err := xmlquery.Parse( strings.NewReader( body ) )
	if err != nil {
		return err
	}

	status := xmlquery.FindOne( doc, "//response/lst[@name='responseHeader']/int[@name='status']")
	if status == nil {
		return fmt.Errorf( "Cannot find status field in response payload (%s)", body )
	}

	if status.InnerText( ) != "0" {
		log.Printf( body )
		return fmt.Errorf( "SOLR response status = %s", status.InnerText( ) )
	}

	//log.Printf("SOLR commit...")
	return nil
}

func ( s * solrImpl ) protocolAdd( builder strings.Builder ) error {

	body, err := s.httpPost( builder.String( ) )

	if err != nil {
		return err
	}

	// look for the response

	doc, err := xmlquery.Parse( strings.NewReader( body ) )
	if err != nil {
		return err
	}

	status := xmlquery.FindOne( doc, "//response/lst[@name='responseHeader']/int[@name='status']")
    if status == nil {
		return fmt.Errorf( "Cannot find status field in response payload (%s)", body )
	}

	if status.InnerText( ) != "0" {

		message := xmlquery.FindOne( doc, "//response/lst[@name='error']/str[@name='msg']")
		if message != nil {
			return fmt.Errorf( "%s", message.InnerText( ) )
		}
		return fmt.Errorf( "status: %s (body %s)", status.InnerText( ), body )
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

//
// end of file
//