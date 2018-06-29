[TOC]

        基于flume + kafka + spark Streaming的流式处理系统
# flume
### 创建配置文件，spark_kafka_agent为agent的名字，接收/Users/zhangyaxing/eclipse-workspace/kafka_spark/spark_kafka.log的数据
        spark_kafka_agent.sources = s1
        spark_kafka_agent.sinks = k1
        spark_kafka_agent.channels = c1
        # spark_kafka_agent : name of agent
        # Describe/configure the source
        spark_kafka_agent.sources.s1.type=exec
        spark_kafka_agent.sources.s1.command=tail -F /Users/zhangyaxing/eclipse-workspace/kafka_spark/spark_kafka.log
        spark_kafka_agent.sources.s1.channels=c1
        # Describe the sink
        spark_kafka_agent.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
        spark_kafka_agent.sinks.k1.topic = spark_kafka_topic  
        spark_kafka_agent.sinks.k1.brokerList = localhost:9092,localhost:9093,localhost:9094
        agent.sinks.k1.serializer.class=kafka.serializer.StringEncoder  
        spark_kafka_agent.sinks.k1.channel = c1 
        # Use a channel which buffers events in memory
        spark_kafka_agent.channels.c1.type = memory
        spark_kafka_agent.channels.c1.capacity = 10000
        spark_kafka_agent.channels.c1.transactionCapacity = 100
        # Bind the source and sink to the channel
        spark_kafka_agent.sources.r1.channels = c1
        spark_kafka_agent.sinks.k1.channel = c1
### 启动flume连接到kafka
        flume-ng agent -n spark_kafka_agent -c conf/ -f conf/spark_kafka.properties -Dflume.root.logger=INFO,console  


# kafka
### 配置文件
    仅仅修改server.properties文件，把\config\server.properties文件复制2份，分别是server.properties，server_1.properties，server_2.properties，

    0.文件server.properties
        broker.id=0
        listeners=PLAINTEXT://localhost:9092
        log.dirs=/tmp/kafka-logs-0
        num.partitions=3
        zookeeper.connect=localhost:2184,localhost:2182,localhost:2183
 
    1.文件server_1.properties
        broker.id=1
        listeners=PLAINTEXT://localhost:9093
        log.dirs=/tmp/kafka-logs-1
        num.partitions=3
        zookeeper.connect=localhost:2184,localhost:2182,localhost:2183

    2.文件server_2.properties
        broker.id=2
        listeners=PLAINTEXT://localhost:9094
        log.dirs=/tmp/kafka-logs-2
        num.partitions=3
        zookeeper.connect=localhost:2184,localhost:2182,localhost:2183

### 启动kafka broker
    bin/kafka-server-start.sh -daemon config/server.properties
    bin/kafka-server-start.sh -daemon config/server_1.properties
    bin/kafka-server-start.sh -daemon config/server_2.properties
### 创建topic
    bin/kafka-topics.sh --create --zookeeper localhost:2182 --replication-factor 3 --partitions 1 --topic spark_kafka_topic
### 创建后，使用 –describe 来查看一下
    bin/kafka-topics.sh --describe --zookeeper localhost:2182 --topic spark_kafka_topic
### 启动kafka消费者接受flume数据
        kafka-console-consumer.sh --zookeeper localhost:2182,localhost:2183,localhost:2184 --topic spark_kafka_topic --from-beginning


# kafka -----> spark streaming
### 什么是Spark Streaming
>流式处理是把连续不断的数据输入分割成单元数据块来处理。
Spark Streaming对Spark核心API进行了相应的扩展，支持高吞吐、低延迟、可扩展的流式数据处理。

### 自己管理offset
>为了让Spark Streaming消费kafka的数据不丢数据，可以创建Kafka Direct DStream，由Spark Streaming自己管理offset，并不是存到zookeeper。启用S​​park Streaming的 checkpoints是存储偏移量的最简单方法，因为它可以在Spark的框架内轻松获得。 checkpoints将应用程序的状态保存到HDFS，以便在故障时可以恢复。如果发生故障，Spark Streaming应用程序可以从checkpoints偏移范围读取消息。但是，Spark Streaming checkpoints在应用程序修改后由于从checkpoint反序列化失败而无法恢复，因此不是非常可靠，特别是如果您将此机制用于关键生产应用程序，另外，基于zookeeper的offset可视化工具将无法使用。我们不建议通过Spark checkpoints来管理偏移量。因此本文将手动存储offset到zookeeper，完全自我掌控offset。

### 编写代码
    KafkaManager.scala
```scala
package org.apache.spark.streaming.kafka
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import scala.reflect.ClassTag
class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {
  // KafkaCluster in Spark is overwrited by myself
  private val kc = new KafkaCluster(kafkaParams)
  //根据offset创建DStream
  def createDirectStream[K: ClassTag,
                        V: ClassTag,
                        KD <: Decoder[K]: ClassTag,
                        VD <: Decoder[V]: ClassTag
                        ](ssc: StreamingContext,
                          kafkaParams: Map[String, String],
                          topics: Set[String]): InputDStream[(K, V, String)] = {
    val groupId = kafkaParams.get("group.id").get
    //在zookeeper上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)
    //从zookeeper上读取offset开始消费message
    val messages = {
      val partitionsE = kc.getPartitions(topics)
      if(partitionsE.isLeft)
        // s"xx ${}" 字符串插值
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if(consumerOffsetsE.isLeft)
        throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
      val consumerOffsets = consumerOffsetsE.right.get
      //从指定offsets处消费kafka
      //messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      //MessageAndMetadata里包含message的topic message等信息
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V, String)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message, mmd.topic)
      )
    }
    messages
  }
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit ={
    topics.foreach(topic => {
      var hasConsumerd = true
      val partitionsE = kc.getPartitions(Set(topic))
      if(partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if(consumerOffsetsE.isLeft) hasConsumerd = false
      //某个groupid首次没有offset信息，会报错从头开始读
      if(hasConsumerd){ // 消费过
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException
          * 说明zk上保存的offsets已经过时，即kafka的定时清理策略已经将包含该offsets的文件删除
          * 针对这种情况，只要判断一下zk伤的consumerOffsets和earliestLeaderOffsets的大小
          * 如果consumerOffsets比earliestLeaderOffsets小的话，说明consumerOffsets过时
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if(earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get
        //可能只存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({case(tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if(n < earliestLeaderOffset){
            println("consumer group: " + groupId + ",topic: " + tp.topic + ",partition: " + tp.partition +
            " offsets已过时，更新为: " + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (!offsets.isEmpty){
          kc.setConsumerOffsets(groupId, offsets)
        }
      }else{ // 没有消费过
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if(reset == Some("smallest")){ // 从头消费
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if(leaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }else{ // 从最新offset处消费
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if(leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map{
          case(tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }
  def updateZKOffsets(rdd: RDD[(String, String, String)]): Unit ={
    val groupId = kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for(offsets <- offsetsList){
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if(o.isLeft){
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }
}
```

    SparkKafkaStreaming.scala
```scala
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.rdd.RDD
object SparkKafkaStreaming {
  def processRdd(rdd: RDD[(String, String, String)]): Unit = {
    rdd.foreach(println)
  }
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
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))
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
      ssc, kafkaParams, topicsSet
    )
    messages.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        processRdd(rdd)
        km.updateZKOffsets(rdd)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
```

# hbase
### 配置文件
```xml
    <configuration>
    //Here you have to set the path where you want HBase to store its files.
    <property>
    <name>hbase.rootdir</name>
    <value>hdfs://localhost:9000/hbase</value>
    </property>
    //Here you have to set the path where you want HBase to store its built
    in zookeeper files.
    <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/usr/local/zk1/data</value>
    </property>
    <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
    </property>
    <property>
    <name>hbase.tmp.dir</name>
    <value>/hbase/tmp</value>
    <description>Temporary directory on the local filesystem.</description>
    </property>
    <property>
      <name>hbase.zookeeper.property.maxClientCnxns</name>
      <value>35</value>
    </property>
    <property>
    <name>hbase.zookeeper.quorum</name>
    <value>localhost:2182,localhost:2183,localhost:2184</value>
    </property>
    </configuration>
```
### 启动hbase服务
    start-hbase.sh
### 启动zookeeper的node
    hbase-daemon.sh start master
### 启动命令行交互模式
    hbase shell
### 代码
```scala
  val hbaseconf = HBaseConfiguration.create()
  hbaseconf.set("hbase.zookeeper.quorum", "localhost")
  hbaseconf.set("hbase.zookeeper.property.clientPort", "2182")
  val table = new HTable(hbaseconf, tablename)
  val theput = new Put(Bytes.toBytes("001"))
  val family = "info"
  val qualifier = "name"
  val value = "xerxes"
  theput.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value))
  table.put(theput)
  table.close()
  ```