package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"regexp"
	"strconv"

	"github.com/antchfx/xmlquery"
)

var maxHttpRetries = 3
var retrySleepTime = 100 * time.Millisecond

var ErrDocumentAdd = fmt.Errorf("single document add failed")
var ErrAllDocumentAdd = fmt.Errorf("all document add failed")

func (s *solrImpl) protocolCommit() error {

	body, err := s.httpPost([]byte("<commit/>"))
	if err != nil {
		return err
	}

	_, _, err = s.processResponsePayload(body)
	if err != nil {
		return err
	}

	//log.Printf("SOLR commit...")
	return nil
}

func (s *solrImpl) protocolPing() error {

	_, err := s.httpGet(s.PingUrl)
	return err
}

func (s *solrImpl) protocolAdd(buffer []byte) (string, error) {

	body, err := s.httpPost(buffer)

	switch err {

	// no error, we need to look at the body to determine if there were specific document failures
	case nil:

		var docNum string
		_, docNum, err = s.processResponsePayload(body)
		if err != nil {

			// one of the documents in the add list failed
			if err == ErrDocumentAdd {
				log.Printf("ERROR: add document number %d FAILED", docNum)
				return docNum, err
			}

			return "", err
		}
		// all good
		return "", nil

	// all the adds failed, the body will tell us which document ID is the problem
	case ErrAllDocumentAdd:

		// we ignore the error from this call because we have already decided that all the documents have failed
		_, docNum, _ := s.processResponsePayload(body)
		// special case here...
		log.Printf("WARNING: all document rejected due to id/doc number %s", docNum)
		return docNum, ErrAllDocumentAdd

	default:
		return "", err
	}
}

func (s *solrImpl) httpGet(url string) ([]byte, error) {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	var response *http.Response
	count := 0
	for {
		response, err = s.httpClient.Do(req)

		count++
		if err != nil {
			if s.canRetry(err) == false {
				return nil, err
			}

			// break when tried too many times
			if count >= maxHttpRetries {
				return nil, err
			}

			log.Printf("ERROR: GET failed with error, retrying (%s)", err)

			// sleep for a bit before retrying
			time.Sleep(retrySleepTime)
		} else {

			defer response.Body.Close()

			body, err := ioutil.ReadAll(response.Body)

			// happy day, hopefully all is well
			if response.StatusCode == http.StatusOK {

				// if the body read failed
				if err != nil {
					log.Printf("ERROR: body read failed (%s)", err)
					return nil, err
				}

				return body, nil
			}

			log.Printf("ERROR: GET failed with status %d (%s)", response.StatusCode, body)

			return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
		}
	}
}

func (s *solrImpl) httpPost(buffer []byte) ([]byte, error) {

	var response *http.Response
	count := 0

	for {
		req, err := http.NewRequest("POST", s.PostUrl, bytes.NewBuffer(buffer))
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/xml")

		response, err = s.httpClient.Do(req)
		count++
		if err != nil {
			if s.canRetry(err) == false {
				return nil, err
			}

			// break when tried too many times
			if count >= maxHttpRetries {
				return nil, err
			}

			log.Printf("WARNING: POST failed with error, retrying (%s)", err)

			// sleep for a bit before retrying
			time.Sleep(retrySleepTime)
		} else {

			defer response.Body.Close()

			body, err := ioutil.ReadAll(response.Body)

			// happy day, hopefully all is well
			if response.StatusCode == http.StatusOK {

				// if the body read failed
				if err != nil {
					log.Printf("ERROR: body read failed (%s)", err)
					return nil, err
				}

				// everything went OK
				return body, nil
			}

			log.Printf("ERROR: POST failed with status %d (%s)", response.StatusCode, body)

			// this is a special case where SOLR rejects all documents
			if response.StatusCode == http.StatusBadRequest {
				return body, ErrAllDocumentAdd
			} else {
				return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
			}
		}
	}
}

func (s *solrImpl) processResponsePayload(body []byte) (int, string, error) {

	// generate a query structure from the body
	doc, err := xmlquery.Parse(bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}

	// attempt to extract the statusNode field
	statusNode := xmlquery.FindOne(doc, "//response/lst[@name='responseHeader']/int[@name='status']")
	if statusNode == nil {
		return 0, "", fmt.Errorf("cannot find status field in response payload (%s)", body)
	}

	// if it appears that we have an error
	if statusNode.InnerText() != "0" {

		// extract the status and attempt to find the error messageNode body
		status, _ := strconv.Atoi(statusNode.InnerText())

		messageNode := xmlquery.FindOne(doc, "//response/lst[@name='error']/str[@name='msg']")
		if messageNode != nil {

			// if this is an error on a specific document number, we try to extract that information
			re := regexp.MustCompile(`\[(\d+),\d+\]`)
			match := re.FindStringSubmatch(messageNode.InnerText())
			if match != nil {
				//fmt.Printf("%s", body)
				// return the document number of failing item
				return status, match[1], ErrDocumentAdd
			}

			// if this is an error on a specific document id, we try to extract that information
			re = regexp.MustCompile(`\[doc=(.+?)\]`)
			match = re.FindStringSubmatch(messageNode.InnerText())
			if match != nil {
				//fmt.Printf("%s", body)
				// return document id of failing item
				return status, match[1], ErrAllDocumentAdd
			}
		}
		return status, "", fmt.Errorf("%s", body)
	}

	// all good
	return 0, "", nil
}

// examines the error and decides if if can be retried
func (s *solrImpl) canRetry(err error) bool {

	if strings.Contains(err.Error(), "operation timed out") == true {
		return true
	}

	if strings.Contains(err.Error(), "Client.Timeout exceeded") == true {
		return true
	}

	if strings.Contains(err.Error(), "write: broken pipe") == true {
		return true
	}

	if strings.Contains(err.Error(), "no such host") == true {
		return true
	}

	if strings.Contains(err.Error(), "network is down") == true {
		return true
	}

	return false
}

//
// end of file
//
