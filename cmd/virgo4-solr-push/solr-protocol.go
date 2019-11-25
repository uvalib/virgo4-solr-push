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

var ErrDocumentAdd = fmt.Errorf("document add failed")

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

	_, err := s.httpGet( s.PingUrl )
	return err
}

func (s *solrImpl) protocolAdd(buffer []byte) (uint, error) {

	body, err := s.httpPost(buffer)
	if err != nil {
		return 0, err
	}

	_, docIx, err := s.processResponsePayload(body)
	if err != nil {

		// one of the documents in the add list failed
		if err == ErrDocumentAdd {
			// special case here...
			log.Printf("ERROR: add document at index %d FAILED", docIx)
			return docIx, err
		}

		return 0, err
	}

	// all good
	return 0, nil
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

			if response.StatusCode != http.StatusOK {
				log.Printf("ERROR: GET failed with status %d", response.StatusCode)

				body, _ := ioutil.ReadAll(response.Body)

				return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
			} else {
				body, err := ioutil.ReadAll(response.Body)
				if err != nil {
					return nil, err
				}

				return body, nil
			}
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

			if response.StatusCode != http.StatusOK {
				log.Printf("ERROR: POST failed with status %d", response.StatusCode)

				body, _ := ioutil.ReadAll(response.Body)

				return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
			} else {
				body, err := ioutil.ReadAll(response.Body)
				if err != nil {
					return nil, err
				}

				return body, nil
			}
		}
	}
}

func (s *solrImpl) processResponsePayload(body []byte) (int, uint, error) {

	// generate a query structure from the body
	doc, err := xmlquery.Parse(bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}

	// attempt to extract the statusNode field
	statusNode := xmlquery.FindOne(doc, "//response/lst[@name='responseHeader']/int[@name='status']")
	if statusNode == nil {
		return 0, 0, fmt.Errorf("cannot find status field in response payload (%s)", body)
	}

	// if it appears that we have an error
	if statusNode.InnerText() != "0" {

		// extract the status and attempt to find the error messageNode body
		status, _ := strconv.Atoi(statusNode.InnerText())

		messageNode := xmlquery.FindOne(doc, "//response/lst[@name='error']/str[@name='msg']")
		if messageNode != nil {

			// if this is an error on a specific document, we can extract that information
			re := regexp.MustCompile(`\[(\d+),\d+\]`)
			match := re.FindStringSubmatch(messageNode.InnerText())
			if match != nil {
				fmt.Printf("%s", body)
				docnum, _ := strconv.Atoi(match[1])

				// return index of failing item
				return status, uint(docnum) - 1, ErrDocumentAdd
			}
		}
		return status, 0, fmt.Errorf("%s", body)
	}

	// all good
	return 0, 0, nil
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
