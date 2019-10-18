package main

import (
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

// time to wait for inbound messages before doing something else
var waitTimeout = 5 * time.Second

func worker(id int, config *ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, inbound <-chan awssqs.Message) {

	// create our SOLR instance
	solr, err := NewSolr(id, *config)
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
			break
		case <-time.After(waitTimeout):
			break
		}

		// we have an inbound message to process
		if arrived == true {

			// buffer it to SOLR
			err = solr.BufferDoc(message.Payload)
			fatalIfError(err)

			// add it to the queued list
			queued = append(queued, message)
		}

		// check to see if it is time to 'add' these to SOLR
		if solr.IsTimeToAdd() == true {

			// add them
			failedIx, err := solr.ForceAdd()
			if err != nil && err != documentAddFailed {
				fatalIfError(err)
			}

			// one of the documents failed to add
			if err == documentAddFailed {

				// how many do we have total
				sz := uint(len(queued))

				// if the failure document was not the last one
				if failedIx < sz {

					log.Printf("INFO: purging documents 0 - %d, ignoring document %d, requeuing %d - %d",
						failedIx-1, failedIx, failedIx+1, sz)

					// delete the ones that succeeded
					err = batchDelete(id, aws, queue, queued[0:failedIx])
					fatalIfError(err)

					// ignore the one that failed and keep the remainder for the next
					queued = queued[failedIx+1:]

				} else {
					log.Printf("INFO: last document in batch of %d failed, ignoring it", sz)

					// delete all but the last of them of them
					err = batchDelete(id, aws, queue, queued[0:sz])
					fatalIfError(err)

					// clear the queue
					queued = queued[:0]
				}
			} else {

				// delete all of them
				err = batchDelete(id, aws, queue, queued)
				fatalIfError(err)

				// clear the queue
				queued = queued[:0]
			}
		}

		// is it time to send a commit to SOLR
		if solr.IsTimeToCommit() == true {
			err = solr.ForceCommit()
			fatalIfError(err)
		}
	}
}

func batchDelete(id int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []awssqs.Message) error {

	// ensure there is work to do
	count := uint(len(messages))
	if count == 0 {
		return nil
	}

	//log.Printf( "About to delete block of %d", count )

	start := time.Now()

	// we do delete in blocks of awssqs.MAX_SQS_BLOCK_COUNT
	fullBlocks := count / awssqs.MAX_SQS_BLOCK_COUNT
	remainder := count % awssqs.MAX_SQS_BLOCK_COUNT

	// go through the inbound messages a 'block' at a time
	for bix := uint(0); bix < fullBlocks; bix++ {

		// calculate slice range
		start := bix * awssqs.MAX_SQS_BLOCK_COUNT
		end := start + awssqs.MAX_SQS_BLOCK_COUNT

		//log.Printf( "Deleting slice [%d:%d]", start, end )

		// and delete them
		err := blockDelete(aws, queue, messages[start:end])
		if err != nil {
			return err
		}
	}

	// handle any remaining
	if remainder != 0 {

		// calculate slice range
		start := fullBlocks * awssqs.MAX_SQS_BLOCK_COUNT
		end := start + remainder

		//log.Printf( "Deleting slice [%d:%d]", start, end )

		// and delete them
		err := blockDelete(aws, queue, messages[start:end])
		if err != nil {
			return err
		}
	}

	duration := time.Since(start)
	log.Printf("Worker %d: batch delete completed in %0.2f seconds", id, duration.Seconds())

	return nil
}

func blockDelete(aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []awssqs.Message) error {

	// delete the block
	opStatus, err := aws.BatchMessageDelete(queue, messages)
	if err != nil {
		if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err
		}
	}

	// did we fail
	if err == awssqs.OneOrMoreOperationsUnsuccessfulError {
		for ix, op := range opStatus {
			if op == false {
				log.Printf("ERROR: message %d failed to delete", ix)
			}
		}
	}

	return nil
}

//
// end of file
//
