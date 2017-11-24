
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * This application reads a stream of words and outputs for every window the three
  * words with most occurrences (= top 3).
  *
  * This program connects to a server socket and reads strings from the socket.
  * The easiest way to try this out is to open a text sever (at port 9000)
  * using the ''netcat'' tool via
  * {{{
  * nc -l -p 9000
  * }}}
  */
object TopNWindow {

  /** Main program method */
  def main(args: Array[String]): Unit = {

    // the host and the port to connect to
    var hostname: String = "localhost"
    var port: Int = 9000

    /** Data type for words with count */
    case class WordWithCount(word: String, count: Long)

    // get the execution environment
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

    class Top3WindowFunction
        extends WindowFunction[WordWithCount, String, String, TimeWindow] {

      def apply(key: String,
                window: TimeWindow,
                input: Iterable[WordWithCount],
                out: Collector[String]): Unit = {
        val tmp: Map[String, Iterable[WordWithCount]] =
          input.groupBy(t => t.word)
        val tmp2: Seq[(String, Int)] =
          tmp.mapValues(_.size).toSeq.sortBy(-_._2).toList.take(3)
        val res = tmp2.toString
        out.collect(res)
      }
    }

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts: WindowedStream[WordWithCount, String, TimeWindow] = text
      .flatMap { w =>
        w.split("\\s")
      }
      .map { w =>
        WordWithCount(w, 1)
      }
      .keyBy(t => "all")
      .window(
        SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))



    def top3(s: String,
       tw: TimeWindow,
       //input: Iterable[(String, Int)],
       input: Iterable[WordWithCount],
       co: Collector[String]): Unit = {
        {
          val tmp: Map[String, Iterable[WordWithCount]] =
            input.groupBy(t => t.word)
          val tmp2: Seq[(String, Int)] =
            tmp.mapValues(_.size).toSeq.sortBy(-_._2).toList.take(3)
          val res = tmp2.toString
          co.collect(res)
        }
    }

    // val aggr = windowCounts.apply { top3 _ }
    val aggr = windowCounts.apply(new Top3WindowFunction())

    aggr.print().setParallelism(1)

    env.execute("Top-N-Window")
  }
}
