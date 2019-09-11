package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"os"
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
    for {

		log.Printf("Waiting for messages...")

		result, err := svc.ReceiveMessage( &sqs.ReceiveMessageInput{
			//AttributeNames: []*string{
			//	aws.String( sqs.QueueAttributeNameAll ),
			//},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll ),
			},
			QueueUrl:            queueUrl,
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64( cfg.PollTimeOut ),
		})

		if err != nil {
			log.Fatal( err )
		}

		// print and then delete
		if len( result.Messages ) != 0 {

			log.Printf( "Received %d messages", len( result.Messages ) )

			for _, m := range result.Messages {

				//log.Printf( "Received %s", *m.Body )
				//for k, v := range m.MessageAttributes {
				//	log.Printf( "(%s = %s)", k, *v.StringValue )
				//}
				_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      queueUrl,
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
