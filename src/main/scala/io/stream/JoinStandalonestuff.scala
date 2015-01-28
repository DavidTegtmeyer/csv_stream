package io.stream

import java.io.{PrintWriter, File}
/**
 * Created by david on 24.01.15.
 */
object JoinStandalonestuff extends App {

  println("Started")
  var buliSource = scala.io.Source.fromFile("/home/david/Dokumente/MA_prod/streams/streams-tutorial/target/data/results/eventIds-for-buli.csv")
  val buliEvents = buliSource.getLines().toIndexedSeq
  var oddsHistorySource = scala.io.Source.fromFile("/home/david/Dokumente/MA_prod/EBET-816/Originale/odds-history-2015-01-14.csv") // 23Gig

  var lastCheckedEvent: String = ""
  
  def eventInBuli(s: String): Boolean = {
    val eventId = s.split(";")(1)

    if(!(eventId == lastCheckedEvent))
    {
      var contained = buliEvents.contains(eventId)
      if (contained)
        lastCheckedEvent = ""
      else
        lastCheckedEvent = eventId
      contained
    } else false
  }

  def join(it: Iterator[String]) = {
    it.filter(s => eventInBuli(s))
  }


  def go() = {
    var outfile = new File("/home/david/Dokumente/MA_prod/streams/streams-tutorial/target/data/results/out.csv")
    val p = new PrintWriter(outfile)
    for (filteredLine <- join(oddsHistorySource.getLines()))
    {
      p.write(filteredLine+"\n")
    }
    p.close()
  }

  def timed[A](block: => A) = {
    val t0 = System.currentTimeMillis
    val result = block
    println("Took %d milliseconds".format((System.currentTimeMillis - t0)))
    result
  }

  timed(go())

  println("Finished")
}
