package main

import (
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"strconv"
	"strings"
	"time"
)

// time to wait for inbound messages before doing something else
var waitTimeout = 5 * time.Second

func worker(workerId int, config *ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, inbound <-chan awssqs.Message) {

	// create our SOLR instance
	solr, err := NewSolr(workerId, *config)
	fatalIfError(err)

	// keep a list of the messages queued so we can delete them once they are sent to SOLR
	queued := make([]awssqs.Message, 0, config.SolrBlockCount)
	var message awssqs.Message

	for {

		arrived := false

		// process a message or wait...
		select {
		case message = <-inbound:
			arrived = true

		case <-time.After(waitTimeout):
		}

		// we have an inbound message to process
		if arrived == true {

			// get the message identifier
			id, found := message.GetAttribute(awssqs.AttributeKeyRecordId)
			if found == false {
				id = "unknown"
				log.Printf("WARNING: cannot locate document id, using default")
			}

			// buffer it to SOLR
			err = solr.BufferDoc(id, message.Payload)
			fatalIfError(err)

			// add it to the queued list
			queued = append(queued, message)
		}

		// check to see if it is time to 'add' these to SOLR
		if solr.IsTimeToAdd() == true {

			// we loop here because we try to rebuffer and reprocess any documents that were not processed...

			for {

				// add them
				var failedDoc string
				failedDoc, err = solr.ForceAdd()

				switch err {

				// no error, everything OK
				case nil:
					// delete all of them
					err = batchDelete(workerId, aws, queue, queued)
					fatalIfError(err)

					// clear the queue
					queued = queued[:0]

				// one of the documents failed
				case ErrDocumentAdd:

					// convert the failed document number to a document index
					failedIx, _ := strconv.Atoi(failedDoc)
					failedIx--

					// how many do we have total
					sz := len(queued)

					// if the failure document was the first one
					if failedIx == 0 {

						log.Printf("worker %d: WARNING first document in batch of %d failed, ignoring it and requing the remainder", workerId, sz)

						// ignore the one that failed and keep the remainder
						queued = queued[1:]

						// if the failure document was not the last one
					} else if failedIx < sz {

						log.Printf("worker %d: WARNING purging documents 0 - %d, ignoring document %d, requeuing %d - %d",
							workerId, failedIx-1, failedIx, failedIx+1, sz)

						// delete the ones that succeeded
						err = batchDelete(workerId, aws, queue, queued[0:failedIx])
						fatalIfError(err)

						// ignore the one that failed and keep the remainder
						queued = queued[failedIx+1:]

						// the failure document was the last one
					} else {
						log.Printf("worker %d: WARNING last document in batch of %d failed, ignoring it", workerId, sz)

						// delete all but the last of them of them
						err = batchDelete(workerId, aws, queue, queued[0:sz])
						fatalIfError(err)

						// clear the queue
						queued = queued[:0]
					}

				// all of the adds failed, attempt to handle as best we can...
				case ErrAllDocumentAdd:

					// if we were able to identify the document that failed then we might be able to handle
					// things in a sensible manner. If we cannot, it's all over

					if len(failedDoc) != 0 {

						log.Printf("worker %d: WARNING all documents failed due to workerId/doc number %s, attempting to recover", workerId, failedDoc)

						// if we are configured for sub-document delimiters, this might be a sub-document workerId so
						// attempt to extract the parent document workerId so we can remove it from the block
						if len(config.SubDocIdDelimiter) != 0 {
							parentID := strings.Split(failedDoc, config.SubDocIdDelimiter)
							if parentID[0] != failedDoc {
								failedDoc = parentID[0]
								log.Printf("worker %d: WARNING extracted parent workerId (%s) from workerId/doc number, looks like a sub-document failure ", workerId, failedDoc)
							}
						}

						// iterate through and remove the bad item
						failedItemRemoved := false
						for ix, m := range queued {
							recId, _ := m.GetAttribute(awssqs.AttributeKeyRecordId)
							if recId == failedDoc {
								log.Printf("worker %d: WARNING removed workerId/doc number %s, requing the remainder", workerId, failedDoc)
								queued = append(queued[:ix], queued[ix+1:]...)
								failedItemRemoved = true
								break
							}
						}

						// if we did not remove any items, lets assume that the failed doc is a document *number* instead of a document ID.
						// remove that one from the list. When we are handling a ErrAllDocumentAdd error, the failed document number
						// should *always* be 1 (the first document).

						if failedItemRemoved == false {

							if failedDoc == "1" {
								log.Printf("worker %d: WARNING removed first doc in list, requing the remainder", workerId)
								queued = queued[1:]
							} else {
								log.Printf("worker %d: ERROR cannot locate workerId/doc number %s in our list, abandoning all buffered items", workerId, failedDoc)
								// clear the queue
								queued = queued[:0]
							}
						}
					} else {
						log.Printf("worker %d: ERROR cannot determine workerId/doc number from the reported failure, abandoning all buffered items", workerId)
						// clear the queue
						queued = queued[:0]
					}

				default:
					fatalIfError(err)
				}

				// we have processed all the queued items, break out of the loop
				if len(queued) == 0 {
					break
				}

				// otherwise, re-buffer any that need to be reprocessed and try again
				for _, m := range queued {
					// get the message identifier
					id, found := m.GetAttribute(awssqs.AttributeKeyRecordId)
					if found == false {
						id = "unknown"
						log.Printf("WARNING: cannot locate document id, using default")
					}

					// buffer it to SOLR
					err = solr.BufferDoc(id, m.Payload)
					fatalIfError(err)
				}
			}
		}

		// is it time to send a commit to SOLR
		if solr.IsTimeToCommit() == true {
			err = solr.ForceCommit()
			fatalIfError(err)
		}
	}
}

func batchDelete(workerId int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []awssqs.Message) error {

	// ensure there is work to do
	count := uint(len(messages))
	if count == 0 {
		return nil
	}

	//log.Printf( "worker %d: About to delete block of %d", workerId, count )

	start := time.Now()

	// we do delete in blocks of awssqs.MAX_SQS_BLOCK_COUNT
	fullBlocks := count / awssqs.MAX_SQS_BLOCK_COUNT
	remainder := count % awssqs.MAX_SQS_BLOCK_COUNT

	// go through the inbound messages a 'block' at a time
	for bix := uint(0); bix < fullBlocks; bix++ {

		// calculate slice range
		start := bix * awssqs.MAX_SQS_BLOCK_COUNT
		end := start + awssqs.MAX_SQS_BLOCK_COUNT

		//log.Printf( "worker %d: Deleting slice [%d:%d]", workerId, start, end )

		// and delete them
		err := blockDelete(workerId, aws, queue, messages[start:end])
		if err != nil {
			return err
		}
	}

	// handle any remaining
	if remainder != 0 {

		// calculate slice range
		start := fullBlocks * awssqs.MAX_SQS_BLOCK_COUNT
		end := start + remainder

		//log.Printf( "worker %d: Deleting slice [%d:%d]", workerId, start, end )

		// and delete them
		err := blockDelete(workerId, aws, queue, messages[start:end])
		if err != nil {
			return err
		}
	}

	duration := time.Since(start)
	log.Printf("worker %d: batch delete completed in %0.2f seconds", workerId, duration.Seconds())

	return nil
}

func blockDelete(workerId int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []awssqs.Message) error {

	// delete the block
	opStatus, err := aws.BatchMessageDelete(queue, messages)
	if err != nil {
		if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
			return err
		}
	}

	// did we fail
	if err == awssqs.ErrOneOrMoreOperationsUnsuccessful {
		for ix, op := range opStatus {
			if op == false {
				log.Printf("worker %d: ERROR message %d failed to delete", workerId, ix)
			}
		}
	}

	return nil
}

//
// end of file
//
