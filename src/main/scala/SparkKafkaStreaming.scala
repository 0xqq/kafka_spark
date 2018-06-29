import java.util.Date

import org.apache.hadoop.hbase.util.{Base64, Bytes}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes

object SparkKafkaStreaming {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
           |  <brokers> is a list of one or more Kafka brokers
           |    <topics> is a list of one or more kafka topics to consume from
           |      <groupid> is a consume group
           |
         """.stripMargin
      )
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers, topics, groupId) = args

    //create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.setMaster("local[2]")
    sparkConf.set("spakr.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    //val ssc = new StreamingContext(sparkConf, Durations.seconds(5))
    val ssc = new StreamingContext(sc, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    //create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId, //con_group
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)
    val words = messages.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_+_)

    wordCounts.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        rdd.foreach(println)
        //更新offset到zookeeper中
        //km.updateZKOffsets(rdd)

        var tablename = "testtable"
        rdd.foreachPartition(iter => {
          //iter.foreach(println)
          /*
          val hbaseconf = HBaseConfiguration.create()
          hbaseconf.set("hbase.zookeeper.quorum", "localhost")
          hbaseconf.set("hbase.zookeeper.property.clientPort", "2182")
          val table = new HTable(hbaseconf, tablename)
          val theput = new Put(Bytes.toBytes("001"))
          //val family = "info"
          //val qualifier = "name"
          //val value = "xerxes"
          theput.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value))
          table.put(theput)
          table.close()
          */
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}