package io.stream

/**
 * Created by david on 24.01.15.
 */
object TimeTest extends App {
  def timed[A](block: => A) = {
    val t0 = System.currentTimeMillis
    val result = block

    (result, System.currentTimeMillis - t0)
  }

  var buffSource = scala.io.Source.fromFile("/home/david/Dokumente/MA_prod/streams/streams-tutorial/target/data/results/eventIds-for-buli.csv")

  val res = for (line <- buffSource.getLines().drop(1)) yield line.toInt

  var time:Long = 0
  1 to 34000000 foreach( i => time += timed(res.contains(i) == true)._2
    )

  println(time.toDouble / 1000 + " secs")
}
