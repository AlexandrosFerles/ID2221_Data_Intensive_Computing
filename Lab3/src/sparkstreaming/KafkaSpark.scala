package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it

    // Credits to https://jaceklaskowski.gitbooks.io/spark-streaming/content/spark-streaming-kafka.html#no-receivers
    // for help
    val conf = new SparkConf().setMaster("local[*]").setAppName("ID2221 Lab 3")
    val ssc = new StreamingContext(conf, batchDuration = Seconds(5))

//    val initialRDD = ssc.sparkContext.parallelize(List(("a", 0)))


    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val kafkaTopics= Set("avg")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, kafkaTopics)

    def help_split(pair: String): (String, Double)={

      val splited = pair.split(",")
      (splited(0), splited(1).toInt)

    }

    val pairs = messages.map(_._2).map(help_split)

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Int, Int)]): (String, Double) = {

      val temp_value = value.getOrElse(0)
      val prev_state = state.getOption.getOrElse((0, 0))

      state.update(prev_state._1 + temp_value, prev_state._2+ 1)
      //No need toget or else since there is sure a value is stored in state
      val avg = state.get()._1 / state.get()._2

      (key, avg)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space" ,"avg")

    ssc.checkpoint("./checkpoints")
    ssc.start()
    ssc.awaitTermination()
    session.close()
  }
}
