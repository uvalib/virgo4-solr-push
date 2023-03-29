package main

import (
	"fmt"
	"log"
	"strings"
	"time"
)

func (s *solrImpl) BufferDoc(id string, doc []byte) error {

	// if we have not yet added any documents
	if s.pendingAdds == 0 {

		// create the open tag for the command
		tag := fmt.Sprintf("<%s>", s.Config.SolrMode)

		// if we are doing client side commit withins
		if s.Config.SolrCommitWithinTime != 0 {
			// commit within time is specified in milliseconds
			tag = fmt.Sprintf("<%s commitWithin=\"%d\">", s.Config.SolrMode, s.Config.SolrCommitWithinTime*1000)
		}

		s.addBuffer = append(s.addBuffer, []byte(tag)...)

		// we are only interested in tracking the time for the last add after the first document is actually
		// added to the buffer
		s.lastAdd = time.Now()
	}

	// add the document and the identifier (for logging) and update the document count
	s.addBuffer = append(s.addBuffer, doc...)
	s.pendingAddIds = append(s.pendingAddIds, id)
	s.pendingAdds++

	return nil
}

func (s *solrImpl) IsAlive() error {
	return s.protocolPing()
}

func (s *solrImpl) IsTimeToAdd() bool {

	// if we have no pending adds then no add is required
	if s.pendingAdds == 0 {
		return false
	}

	//
	// its time to add if we have reached the configured record size or the configured buffer size or
	// if we have pending items and we have not added in the configured number of seconds
	//

	if s.pendingAdds >= s.Config.SolrBlockCount {
		log.Printf("worker %d: reached send block count", s.workerId)
		return true
	}

	if len(s.addBuffer) >= int(s.sendBufferSize) {
		log.Printf("worker %d: reached send buffer size", s.workerId)
		return true
	}

	if time.Since(s.lastAdd).Seconds() > (time.Duration(s.Config.SolrFlushTime) * time.Second).Seconds() {
		log.Printf("worker %d: reached send timeout", s.workerId)
		return true
	}

	return false
}

// it is time to commit if we have recently added one or more documents without committing and
// the commit time has elapsed
func (s *solrImpl) IsTimeToCommit() bool {

	// if SOLR is not dirty then no commit is required
	if s.solrDirty == false {
		return false
	}

	// if our commit time is zero, it means that client explicit committing is disabled
	if s.Config.SolrCommitTime == 0 {
		return false
	}

	//
	// its time to commit if we have not committed in the configured number of seconds
	//
	return time.Since(s.lastCommit).Seconds() > (time.Duration(s.Config.SolrCommitTime) * time.Second).Seconds()
}

func (s *solrImpl) ForceAdd() (string, error) {

	// nothing to add
	if s.pendingAdds == 0 {
		return "", nil
	}

	tag := fmt.Sprintf("</%s>", s.Config.SolrMode)
	s.addBuffer = append(s.addBuffer, []byte(tag)...)
	log.Printf("worker %d: sending %d documents to SOLR (buffer %d bytes)", s.workerId, s.pendingAdds, len(s.addBuffer))
	log.Printf("worker %d: ids: %s", s.workerId, strings.Join(s.pendingAddIds, " "))

	// add to SOLR
	start := time.Now()
	failedDoc, err := s.protocolAdd(s.addBuffer)
	duration := time.Since(start)

	switch err {

	// no error
	case nil:

		log.Printf("worker %d: added %d documents in %0.2f seconds", s.workerId, s.pendingAdds, duration.Seconds())

		// only start timing for a SOLR commit after SOLR becomes dirty
		if s.solrDirty == false {
			s.lastCommit = time.Now()
		}

		// update state variables
		s.solrDirty = true

		// clear the buffer and other state variables
		s.addBuffer = s.addBuffer[:0]
		s.pendingAddIds = s.pendingAddIds[:0]
		s.pendingAdds = 0
		s.lastAdd = time.Now()

		return "", nil

	// one of the documents added failed
	case ErrDocumentAdd:

		log.Printf("worker %d: added some documents in %0.2f seconds", s.workerId, duration.Seconds())

		// only start timing for a SOLR commit after SOLR becomes dirty
		if s.solrDirty == false {
			s.lastCommit = time.Now()
		}

		// update state variables
		s.solrDirty = true

		// clear the buffer and other state variables
		s.addBuffer = s.addBuffer[:0]
		s.pendingAddIds = s.pendingAddIds[:0]
		s.pendingAdds = 0
		s.lastAdd = time.Now()

		return failedDoc, ErrDocumentAdd

	// all the document adds failed
	case ErrAllDocumentAdd:

		log.Printf("worker %d: added no documents in %0.2f seconds", s.workerId, duration.Seconds())

		// clear the buffer and other state variables
		s.addBuffer = s.addBuffer[:0]
		s.pendingAddIds = s.pendingAddIds[:0]
		s.pendingAdds = 0
		//s.lastAdd = time.Now()

		return failedDoc, ErrAllDocumentAdd

	// some other error, just return it
	default:
		return failedDoc, err
	}
}

func (s *solrImpl) ForceCommit() error {

	// nothing to commit
	if s.solrDirty == false {
		return nil
	}

	//log.Printf("worker %d: committing SOLR", s.workerId )

	// commit the changes
	start := time.Now()
	err := s.protocolCommit()
	duration := time.Since(start)

	if err != nil {
		return err
	}

	log.Printf("worker %d: commit completed in %0.2f seconds", s.workerId, duration.Seconds())

	// update state variables
	s.lastCommit = time.Now()
	s.solrDirty = false

	return nil
}

//
// end of file
//
