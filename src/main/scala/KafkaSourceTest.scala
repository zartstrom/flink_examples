package main

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.checkpoint
import org.apache.flink.util.{Collector, IterableIterator}
import java.lang

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.Properties
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.{
  SimpleStringSchema,
  SerializationSchema,
  DeserializationSchema
}

object KafkaSourceTest {

  // just keep it as an example
  /*
  object KafkaStringSchema
      extends SerializationSchema[String, Array[Byte]]
      with DeserializationSchema[String] {

    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.api.java.typeutils.TypeExtractor

    override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")

    override def isEndOfStream(t: String): Boolean = false

    override def deserialize(bytes: Array[Byte]): String =
      new String(bytes, "UTF-8")

    override def getProducedType: TypeInformation[String] =
      TypeExtractor.getForClass(classOf[String])
  }
  */

  /** Main program method */
  def main(args: Array[String]): Unit = {

    /** Data type for words with count */
    case class WordWithCount(word: String, count: Long)

    // get the execution environment
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    // val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    // properties.setProperty("zookeeper.connect", "localhost:2181");
    // properties.setProperty("group.id", "test")

    val stream: DataStream[String] = env
      .addSource(
        new FlinkKafkaConsumer010[String]("raw", new SimpleStringSchema(), properties))
    val windowCounts: WindowedStream[WordWithCount, String, TimeWindow] = stream
      .flatMap { w =>
        w.split("\\s")
      }
      .map { w =>
        WordWithCount(w, 1)
      // (w, 1)
      }
      .keyBy(t => t.word)
      //.keyBy("word")
      .window(
        SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))

    val result = windowCounts.sum("count")
    result.print().setParallelism(1)

    env.execute("Kafka Window WordCount")

  }
}
