package main

import (
	"bytes"
	"log"
	"os"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handle from the queue name
	inQueueHandle, err := aws.QueueHandle( cfg.InQueueName )
	if err != nil {
		log.Fatal( err )
	}

	// create the SOLR instance
	err = NewSolr( cfg.SolrUrl, cfg.CoreName )
	if err != nil {
		log.Fatal( err )
	}

	// we write to SOLR in blocks for performance reasons
	var payload bytes.Buffer
	payload.WriteString( "<add>" )
	payload_count := uint( 0 )
	last_flush := time.Now()

	// we need to keep a list of 'receipt handles' so we can delete from the queue
	delete_handles := make( []string, 0, cfg.SolrBlockCount + awssqs.MAX_SQS_BLOCK_COUNT )

	// we commit to SOLR on a time basis
    solr_dirty := false
	last_commit := time.Now()

    for {

		// if SOLR is not dirty then dont start calculating the time since the last commit
		if solr_dirty == false {
			last_commit = time.Now()
		}

		//log.Printf("Waiting for messages...")

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet( inQueueHandle, awssqs.MAX_SQS_BLOCK_COUNT, time.Duration( cfg.PollTimeOut ) * time.Second )
		if err != nil {
			log.Fatal( err )
		}

		// did we receive any?
		sz := len( messages )
		if sz != 0 {

			//log.Printf( "Received %d messages", len( result.Messages ) )

			// combine for a single SOLR payload and save the receipt handles
			for _, m := range messages {
			   payload.WriteString( string( m.Payload ) )
			   delete_handles = append(delete_handles, string( m.DeleteHandle ) )
			   payload_count++
			}

		} else {
			log.Printf("No documents available (%d pending)", payload_count )
		}

		// is it time to send the content to SOLR? Either because we have reached the configured number of records to send
		// or because it has been a certain time since we last flushed the buffer
		since_last_flush := time.Since( last_flush )
		if ( payload_count >= cfg.SolrBlockCount ) || ( payload_count > 0 && since_last_flush.Seconds( ) >= float64( cfg.FlushTime ) ){

			payload.WriteString( "</add>" )
			log.Printf("Sending %d documents to SOLR", payload_count )
			start := time.Now()

			// add to SOLR
			err = Solr.Add(payload.String())

			if err != nil {
				log.Fatal(err)
			}

			// send a batch delete (for performance reasons)
			err = batchDelete( aws, inQueueHandle, delete_handles)

			if err != nil {
				log.Fatal( err )
			}

			duration := time.Since(start)
			log.Printf("Processed %d documents (%0.2f tps)", payload_count, float64(payload_count)/duration.Seconds())

			// reset the counter, buffer and list of receipt handles
			payload.Reset( )
			payload.WriteString( "<add>" )
			payload_count = 0
			delete_handles = delete_handles[:0]

			// save the last flush time and mark SOLR as dirty so we will (eventually) do a commit
			last_flush = time.Now()
			solr_dirty = true
		}

		// is it time to do a commit? note that the last_commit time is set anytime we determine that SOLR is
		// not dirty so the only time that since_last_commit will reach the timeout is if SOLR is dirty
		since_last_commit := time.Since( last_commit )
		if since_last_commit.Seconds( ) >= float64( cfg.CommitTime ) {

			log.Printf("Committing documents" )

			// commit the changes
			err = Solr.Commit()

			if err != nil {
				log.Fatal(err)
			}

			// SOLR is no longer dirty, we will save the last commit time at the top of the loop
			solr_dirty = false
		}
	}
}

func batchDelete( aws awssqs.AWS_SQS, queue awssqs.QueueHandle, delete_handles []string ) error {

	count := len( delete_handles )
	if count == 0 {
		return nil
	}

	block := make( []awssqs.Message, 0, awssqs.MAX_SQS_BLOCK_COUNT )

	// the delete loop, assume everything worked
	for ix, m := range delete_handles {

		block = append(block, constructMessage( m ) )

		// is it time to send a block of deletes
		if uint( ix ) % awssqs.MAX_SQS_BLOCK_COUNT == awssqs.MAX_SQS_BLOCK_COUNT - 1 {

			// delete them all
			opStatus, err := aws.BatchMessageDelete( queue, block )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to delete", ix )
				}
			}

			block = block[:0]
		}
	}

	// do we have pending messages (not on a block boundary)
    if len(block) != 0 {

		// delete them all
		opStatus, err := aws.BatchMessageDelete( queue, block )
		if err != nil {
			log.Fatal( err )
		}

		// check the operation results
		for ix, op := range opStatus {
			if op == false {
				log.Printf( "WARNING: message %d failed to delete", ix )
			}
		}
	}

	return nil
}

func constructMessage( message string ) awssqs.Message {

	return awssqs.Message{ DeleteHandle: awssqs.DeleteHandle( message )}
}

//
// end of file
//