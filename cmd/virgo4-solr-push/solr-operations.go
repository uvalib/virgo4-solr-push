package main

import (
   "log"
   "time"
)

func ( s * solrImpl ) BufferDoc( doc []byte ) error {

   // if we have not yet added any documents
   if s.pendingAdds == 0 {
      s.addBuffer = append( s.addBuffer, []byte( "<add>" )... )

      // we are only interested in tracking the time for the last add after the first document is actually
      // added to the buffer
      s.lastAdd = time.Now( )
   }

   // add the document and update the document count
   s.addBuffer = append( s.addBuffer, doc... )
   s.pendingAdds++

   return nil
}

func ( s * solrImpl ) IsAlive( ) error {
   //_, _, err := s.si.Ping()
   return nil
}

func ( s * solrImpl ) IsTimeToAdd( ) bool {

   // if we have no pending adds then no add is required
   if s.pendingAdds == 0 {
      return false
   }

   //
   // its time to add if we have reached the configured block size or of we have pending items and we
   // have not added in the configured number of seconds
   //
   return s.pendingAdds >= s.Config.MaxBlockCount || time.Since(s.lastAdd).Seconds() > s.Config.FlushTime.Seconds()
}

// it is time to commit if we have recently added one or more documents without committing and
// the commit time has elapsed
func ( s * solrImpl ) IsTimeToCommit( ) bool {

   // if SOLR is not dirty then no commit is required
   if s.solrDirty == false {
      return false
   }

   // if our commit time is zero, it means that client committing is disabled
   if s.Config.CommitTime.Seconds() == 0 {
      return false
   }

   //
   // its time to commit if we have not committed in the configured number of seconds
   //
   return time.Since(s.lastCommit).Seconds() > s.Config.CommitTime.Seconds()
}

func ( s * solrImpl ) ForceAdd( ) error {

   // nothing to add
   if s.pendingAdds == 0 {
      return nil
   }

   s.addBuffer = append( s.addBuffer, []byte( "</add>" )... )
   //log.Printf("Worker %d: sending %d documents to SOLR", s.workerId, s.pendingAdds )

   // add to SOLR
   start := time.Now()
   err := s.protocolAdd( s.addBuffer )
   duration := time.Since( start )

   if err != nil {
      return err
   }

   log.Printf("Worker %d: added %d documents in %0.2f seconds", s.workerId, s.pendingAdds, duration.Seconds( ) )

   // only start timing for a SOLR commit after SOLR becomes dirty
   if s.solrDirty == false {
      s.lastCommit = time.Now( )
   }

   // update state variables
   s.solrDirty = true
   s.addBuffer = s.addBuffer[:0]
   s.pendingAdds = 0
   s.lastAdd = time.Now( )

   return nil
}

func ( s * solrImpl ) ForceCommit( ) error {

   // nothing to commit
   if s.solrDirty == false {
      return nil
   }

   //log.Printf("Worker %d: committing SOLR", s.workerId )

   // commit the changes
   start := time.Now()
   err := s.protocolCommit()
   duration := time.Since( start )

   if err != nil {
      return err
   }

   log.Printf("Worker %d: commit completed in %0.2f seconds", s.workerId, duration.Seconds( ) )

   // update state variables
   s.lastCommit = time.Now( )
   s.solrDirty = false

   return nil
}


//
// end of file
//
