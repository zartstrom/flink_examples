import java.lang

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import collection.JavaConversions._
import collection.JavaConverters._

object DistinctWordCount {

  def main(args: Array[String]): Unit = {

    // the host and the port to connect to
    var hostname: String = "localhost"
    var port: Int = 9000

    /** Data type for words with count */
    case class WordWithCount(word: String, count: Long)

    // get the execution environment
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

    val windowCounts: WindowedStream[WordWithCount, String, TimeWindow] = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy(t => "all")
      .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))

    def distinctCount(s: String, tw: TimeWindow, input: Iterable[WordWithCount], out: Collector[String]): Unit = {
      val discount = input.map(t => t.word).toSet.size
      out.collect(s"Distinct elements: $discount")
    }

    class DiscountWindowFunction extends WindowFunction[WordWithCount, String, String, TimeWindow] {
      def apply(key: String, window: TimeWindow, input: Iterable[WordWithCount], out: Collector[String]): Unit = {
        val discount = input.map(t => t.word).toSet.size
        out.collect(s"Distinct elements: $discount")
      }
    }

    // val distinctCountStream: DataStream[String] = windowCounts.apply { distinctCount _ } // compiles
    val distinctCountStream = windowCounts.apply(new DiscountWindowFunction())

    distinctCountStream.print().setParallelism(1)

    env.execute("DistinctWordCount")
  }
}
