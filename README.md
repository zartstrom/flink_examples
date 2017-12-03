Some basic and complete examples of flink stream applications. Complete in the sense that: 
* `build.sbt` contains all the necessary dependencies,
* imports are set correctly and
* code compiles.

All examples are modifications of the basic [WordCount example](https://ci.apache.org/projects/flink/flink-docs-release-1.3/quickstart/setup_quickstart.html) on flink's quickstart page.


#### Distinct Word Count
In [DistinctWordCount.scala](https://github.com/zartstrom/flink_examples/blob/master/src/main/scala/DistinctWordCount.scala) we count distinct words in a time window via a window function.

```scala
class DistinctCountWindowFunction extends WindowFunction[WordWithCount, String, String, TimeWindow]
```

#### Top N in Window   

In [TopNWindow.scala](https://github.com/zartstrom/flink_examples/blob/master/src/main/scala/TopNWindow.scala) we count occurrences of words and calculate the top n words by count in a time window.

#### Kafka source

[KafkaSourceTest.scala](https://github.com/zartstrom/flink_examples/blob/master/src/main/scala/KafkaSourceTest.scala) fetches data from a kafka topic using a flink kafka consumer.

```scala
new FlinkKafkaConsumer010[String]("raw", new SimpleStringSchema(), properties))
```
