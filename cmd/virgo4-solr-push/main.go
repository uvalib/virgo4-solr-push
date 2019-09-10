package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
)

//
// main entry point
//
func main() {

	//log.Printf("===> V4 SOLR push service staring up <===")

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	sess, err := session.NewSession( )
	if err != nil {
		log.Fatal( err )
	}

	svc := sqs.New(sess)

    for {

		log.Printf("Waiting for messages...")

		result, err := svc.ReceiveMessage( &sqs.ReceiveMessageInput{
			//AttributeNames: []*string{
			//	aws.String( sqs.QueueAttributeNameAll ),
			//},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll ),
			},
			QueueUrl:            &cfg.QueueUrl,
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64( cfg.PollTimeOut ),
		})

		if err != nil {
			log.Fatal( err )
		}

		// print and then delete
		if len( result.Messages ) != 0 {

			for _, m := range result.Messages {
				log.Printf( "Received %s", *m.Body )
				for k, v := range m.MessageAttributes {
					log.Printf( "(%s = %s)", k, *v.StringValue )
				}
				_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &cfg.QueueUrl,
					ReceiptHandle: m.ReceiptHandle,
				})

				if err != nil {
					log.Fatal( err )
				}
			}

		} else {
			log.Printf("No messages received...")
		}
	}
}
