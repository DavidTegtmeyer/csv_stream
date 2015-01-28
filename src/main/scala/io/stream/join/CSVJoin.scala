package io.stream.join

import java.io.{PrintWriter, File}

/**
 * Created by david on 25.01.15.
 */

object CSVJoin {
  /*
    Definition of what to join
   */
  def SEPERATOR = ";"
  def leftFile: String = "/home/david/Schreibtisch/a.csv"
  def rightFile: String = "/home/david/Schreibtisch/b.csv"
  def outputFile: String = "/home/david/Schreibtisch/result.csv"
  def leftPos: Int = 1
  def rightPos: Int = 0

  /*
    end of definition
   */
  
  def leftSource = scala.io.Source.fromFile(leftFile)
  def rightSource = scala.io.Source.fromFile(rightFile)
  def rightSourceSeq = rightSource.getLines().toIndexedSeq

  def leftSourceSeq = leftSource.getLines().toIndexedSeq

  def getRightJoinAttribute = getNthElement(rightPos)(_)
  def getLeftJoinAttribute = getNthElement(leftPos)(_)

  def getNthElement(n: Int)(s: String) = s.split(SEPERATOR)(n)

  def filterCriterion(leftPos: Int, rightPos: Int) =
    (leftLine: String) => rightSourceSeq.collect { case s if getRightJoinAttribute(s) == getLeftJoinAttribute(leftLine) => s }

  def definedFilterCriterion = filterCriterion(leftPos, rightPos)

  def processLine(s: String): List[String] = {
    definedFilterCriterion(s).map(leftLine => s + ";" + leftLine) .toList
  }
}
class CSVJoin {
  import CSVJoin._
  
  var lastCheckedEvent: String = ""


  def join(it: Iterator[String]):List[String] =(for (line <- it.toList) yield processLine(line)).flatten

  def printState() = {
    println("State of CSVJoin")
    println("Looking for:")
    leftSourceSeq.map(s => println(getLeftJoinAttribute(s)))
    println("")
    println("In:")
    rightSourceSeq.map(s => println(getRightJoinAttribute(s)))
  }

  def join(): Unit = {

    printState()

    var outfile = new File(outputFile)
    
    val p = new PrintWriter(outfile)
    for (filteredLine <- join(leftSource.getLines()))
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
}

/*

  def rightLineContains(s: String): Boolean = {
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

 */
