package main

import (
   "github.com/uvalib/virgo4-sqs-sdk/awssqs"
   "log"
   "time"
)

// time to wait for inbound messages before doing something else
var waitTimeout = 5 * time.Second

func worker( id int, config * ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, inbound <- chan awssqs.Message, ) {

   // create the SOLR configuration
   solrConfig := SolrConfig{
      EndpointUrl:    config.SolrUrl,
      CoreName:       config.SolrCoreName,
      MaxBlockCount:  config.SolrBlockCount,
      CommitTime:     time.Duration( config.SolrCommitTime) * time.Second,
      FlushTime:      time.Duration( config.SolrFlushTime) * time.Second,
      RequestTimeout: time.Duration( config.SolrTimeout ) * time.Second,
   }

   // and our SOLR instance
   solr, err := NewSolr( id, solrConfig )
   if err != nil {
      log.Fatal( err )
   }

   // keep a list of the messages queued so we can delete them once they are sent to SOLR
   queued := make( []awssqs.Message, 0, config.SolrBlockCount )
   var message awssqs.Message

   for {

      arrived := false

      // process a message or wait...
      select {
      case message = <- inbound:
         arrived = true
         break
      case <- time.After( waitTimeout ):
         break
      }

      // we have an inbound message to process
      if arrived == true {

         // buffer it to SOLR
         err = solr.BufferDoc( []byte( message.Payload ) )
         if err != nil {
            log.Fatal( err )
         }

         // add it to the queued list
         queued = append( queued, message )
      }

      // check to see if it is time to 'add' these to SOLR
      if solr.IsTimeToAdd( ) == true {

         // add them
         err = solr.ForceAdd( )
         if err != nil {
            log.Fatal( err )
         }

         // and do a batch delete
         err = batchDelete( id, aws, queue, queued )
         if err != nil {
            log.Fatal( err )
         }

         queued = queued[:0]
      }

      // is it time to send a commit to SOLR
      if solr.IsTimeToCommit( ) == true {
         err = solr.ForceCommit( )
         if err != nil {
            log.Fatal( err )
         }
      }
   }
}

func batchDelete( id int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []awssqs.Message ) error {

   // ensure there is work to do
   count := uint( len( messages ) )
   if count == 0 {
      return nil
   }

   //log.Printf( "About to delete block of %d", count )

   start := time.Now()

   // we do delete in blocks of awssqs.MAX_SQS_BLOCK_COUNT
   fullBlocks := count / awssqs.MAX_SQS_BLOCK_COUNT
   remainder := count % awssqs.MAX_SQS_BLOCK_COUNT

   // go through the inbound messages a 'block' at a time
   for bix := uint( 0 ); bix < fullBlocks; bix++ {

      // calculate slice range
      start := bix * awssqs.MAX_SQS_BLOCK_COUNT
      end := start + awssqs.MAX_SQS_BLOCK_COUNT

      //log.Printf( "Deleting slice [%d:%d]", start, end )

      // and delete them
      opStatus, err := aws.BatchMessageDelete( queue, messages[ start:end ] )
      if err != nil {
         return err
      }

      // check the operation results
      for ix, op := range opStatus {
         if op == false {
            log.Printf( "WARNING: message %d failed to delete", ix )
         }
      }
   }

   // handle any remaining
   if remainder != 0 {

      // calculate slice range
      start := fullBlocks * awssqs.MAX_SQS_BLOCK_COUNT
      end := start + remainder

      //log.Printf( "Deleting slice [%d:%d]", start, end )

      // and delete them
      opStatus, err := aws.BatchMessageDelete( queue, messages[ start:end ] )
      if err != nil {
         return err
      }

      // check the operation results
      for ix, op := range opStatus {
         if op == false {
            log.Printf( "WARNING: message %d failed to delete", ix )
         }
      }
   }

   duration := time.Since( start )
   log.Printf("Worker %d: batch delete completed in %0.2f seconds", id, duration.Seconds( ) )

   return nil
}

//
// end of file
//