package main

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"os"
	"time"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	sess, err := session.NewSession( )
	if err != nil {
		log.Fatal( err )
	}

	svc := sqs.New(sess)

	// get the queue URL from the name
	result, err := svc.GetQueueUrl( &sqs.GetQueueUrlInput{
		QueueName: aws.String( cfg.InQueueName ),
	})

	if err != nil {
		log.Fatal( err )
	}

	queueUrl := result.QueueUrl

	// create the SOLR instance
	err = NewSolr( cfg.SolrUrl, cfg.CoreName )
	if err != nil {
		log.Fatal( err )
	}

	// we write to SOLR in blocks for performance reasons
	var payload bytes.Buffer
	payload.WriteString( "<add>" )
	payload_count := 0

	// we commit to SOLR on a time basis
	last_commit := time.Now()
    pending_commit := false

    for {

		//log.Printf("Waiting for messages...")

		result, err := svc.ReceiveMessage( &sqs.ReceiveMessageInput{
			//AttributeNames: []*string{
			//	aws.String( sqs.QueueAttributeNameAll ),
			//},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll ),
			},
			QueueUrl:            queueUrl,
			MaxNumberOfMessages: aws.Int64(10),
//			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64( cfg.PollTimeOut ),
		})

		if err != nil {
			log.Fatal( err )
		}

		// print and then delete
		if len( result.Messages ) != 0 {

			//log.Printf( "Received %d messages", len( result.Messages ) )
			//start := time.Now()

			// combine for a single SOLR payload
			for _, m := range result.Messages {
			   payload.WriteString( *m.Body )
			   payload_count++
			}

			// the delete loop, assume everything worked
			for _, m := range result.Messages {

				_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      queueUrl,
					ReceiptHandle: m.ReceiptHandle,
				})

				if err != nil {
					log.Fatal( err )
				}
			}

			// we now have work pending
			pending_commit = true

		} else {
			log.Printf("No records available (%d pending)", payload_count )
		}

		// is it time to send the content to SOLR?
		if payload_count >= cfg.BlockCount {

			payload.WriteString( "</add>" )
			log.Printf("Sending %d records to SOLR", payload_count )
			start := time.Now()

			// add to SOLR
			err = Solr.Add(payload.String())

			if err != nil {
				log.Fatal(err)
			}

			duration := time.Since(start)
			log.Printf("Processed %d records (%0.2f tps)", payload_count, float64(payload_count)/duration.Seconds())

			// reset the counter and buffer
			payload.Reset( )
			payload.WriteString( "<add>" )
			payload_count = 0
		}

		// is it time to do a commit
		since_last_commit := time.Since( last_commit )
		if pending_commit == true && since_last_commit.Seconds( ) >= float64( cfg.CommitTime ) {

			// do we have a pending block to send
			if payload_count > 0 {

				payload.WriteString("</add>")
				log.Printf("Sending %d records to SOLR", payload_count)
				start := time.Now()

				// add to SOLR
				err = Solr.Add(payload.String())

				if err != nil {
					log.Fatal(err)
				}

				duration := time.Since(start)
				log.Printf("Processed %d records (%0.2f tps)", payload_count, float64(payload_count)/duration.Seconds())
			}

			log.Printf("Committing records" )

			// commit the changes
			err = Solr.Commit()

			if err != nil {
				log.Fatal(err)
			}

			last_commit = time.Now( )
			pending_commit = false
		}
	}
}

//
// end of file
//