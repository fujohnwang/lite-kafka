package litekafka

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.{FetchRequestBuilder, PartitionOffsetRequestInfo}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import org.slf4j.LoggerFactory

import collection.JavaConversions._
import scala.beans.BeanProperty


/**
 * a MessageDispatcher will be executed in single thread by LiteMessageConsumer,
 * but it can dispatch messages concurrently or in parallel asynchronously.
 *
 * of course, if sequential message processing is enough, a MessageDispatcher can be used as message handler directly too.
 */
trait TextMessageDispatcher {
  def onMessage(topic: String, key: String, message: String, offset: Long): Unit
}


/**
 * A LiteMessageConsumer will ONLY consume message from partition 0 of any topic it subscribes from.
 *
 * We would like to simplify kafka's message producer/consumer model by degrading the topic to have only 1 partition.
 *
 * this LiteMessageConsumer will NOT handle failover and reconnect things, make one for yourself with the help of this class if you need.
 */
class LiteTextMessageConsumer(brokers: Array[String], topic: String, val messageProcessor: TextMessageDispatcher) {
  val logger = LoggerFactory.getLogger("LiteMessageConsumer")

  val partition: Int = 0

  val running = new AtomicBoolean()

  val textEncoding = Charset.forName("UTF-8")

  @BeanProperty
  var startPosition: Long = 0
  @BeanProperty
  var offsetWhichTime: Long = kafka.api.OffsetRequest.LatestTime

  @BeanProperty
  var clientId: String = "__TRANSIENT_CLIENT_ID__"

  @BeanProperty
  var soTimeout: Int = 10000
  @BeanProperty
  var bufferSize: Int = 1024 * 64
  @BeanProperty
  var fetchSize: Int = 100000
  @BeanProperty
  var fetchIntervalIfEmptyInMilliseconds: Int = 100

  protected var consumer: SimpleConsumer = _


  /**
   * if start position assigned is out of range on the broker, fails fast by throwing exception and exit.
   * recovery exceptions can be caught and handled property, as to other kind of exceptions, just let it fail fast and exit.
   */
  def start(): Unit = {
    // kick off the consumer and dispatch message received to message processor
    val metadata = findLeader()
    if (metadata == null) throw new IllegalStateException(s"Can't find metadata for Topic:$topic and Partition:0.")
    if (metadata.leader == null) throw new IllegalStateException(s"Can't find Leader for Topic:$topic and Partition:0.")

    consumer = new SimpleConsumer(metadata.leader.host, metadata.leader.port, soTimeout, bufferSize, clientId)
    try {
      val startOffset = negotiateOffset(consumer)
      running.compareAndSet(false, true)

      var batchStartOffset = startOffset

      while (running.get()) {
        val fetchRequest = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, batchStartOffset, fetchSize).build()
        val fetchResponse = consumer.fetch(fetchRequest)
        if (fetchResponse.hasError) {
          val errorCode = fetchResponse.errorCode(topic, partition)
          throw new IllegalStateException("Error fetching data from the Broker:" + metadata.leader, ErrorMapping.exceptionFor(errorCode))
        }
        if (fetchResponse.messageSet(topic, partition).isEmpty) {
          logger.debug(s"no more message fetched, sleep $fetchIntervalIfEmptyInMilliseconds ms for next round fetch.")
          TimeUnit.MILLISECONDS.sleep(fetchIntervalIfEmptyInMilliseconds)
        } else {
          for (messageAndOffset <- fetchResponse.messageSet(topic, partition)) {
            val offset = messageAndOffset.offset
            val key = if (!messageAndOffset.message.hasKey) null else readByteBufferToText(messageAndOffset.message.key)
            val message = if (messageAndOffset.message.isNull()) null else readByteBufferToText(messageAndOffset.message.payload)
            messageProcessor.onMessage(topic, key, message, offset)
            batchStartOffset = messageAndOffset.nextOffset
          }
        }
      }
    } finally {
      if (consumer != null) consumer.close()
    }

  }

  def shutdown(): Unit = {
    running.compareAndSet(true, false)
  }

  protected def readByteBufferToText(buffer: ByteBuffer): String = {
    val bytes = new Array[Byte](buffer.limit)
    buffer.get(bytes)
    new String(bytes, textEncoding)
  }

  protected def findLeader(): PartitionMetadata = {
    val topics = Collections.singletonList(topic)
    for (seed <- brokers) {
      var consumer: SimpleConsumer = null
      try {
        val parts = seed.split(':')
        consumer = new SimpleConsumer(parts(0), parts(1).toInt, soTimeout, bufferSize, "leaderFinderFor" + clientId)
        val request = new TopicMetadataRequest(topics)
        val response: kafka.javaapi.TopicMetadataResponse = consumer.send(request)
        for (topicMetadata: kafka.javaapi.TopicMetadata <- response.topicsMetadata) {
          for (partitionMetadata: kafka.javaapi.PartitionMetadata <- topicMetadata.partitionsMetadata) {
            if (partitionMetadata.partitionId == partition) {
              return partitionMetadata
            }
          }
        }
      } catch {
        case ex: Exception => logger.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
          + ", " + partition + "] Reason: " + ex)
      } finally {
        if (consumer != null) consumer.close()
      }
    }
    null
  }

  /**
   * if startPosition is given explicitly, use it; otherwise, use  #{offsetWhichTime} to search a valid offset on the broker.
   * @return a valid offset to be used as start position
   */
  protected def negotiateOffset(consumer: SimpleConsumer): Long = {
    if (startPosition != 0) {
      startPosition
    } else {
      val topicAndPartition = new TopicAndPartition(topic, partition)
      val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(offsetWhichTime, 1))
      val request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientId)
      val response = consumer.getOffsetsBefore(request)
      if (response.hasError) {
        throw new IllegalArgumentException("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition))
      }
      response.offsets(topic, partition)(0)
    }
  }
}


object LiteTextMessageConsumer {
  def main(args: Array[String]) {
    val messageConsumer = new LiteTextMessageConsumer(Array("192.168.1.209:9092"), "csw_nbk_data", new TextMessageDispatcher {
      override def onMessage(topic: String, key: String, message: String, offset: Long): Unit = println(s"receive message from topic:$topic at offset:$offset with key=$key, message=$message")
    })

    messageConsumer.setOffsetWhichTime(kafka.api.OffsetRequest.EarliestTime)
    //    messageConsumer.setStartPosition(6L)
    
    new Thread() {
      override def run(): Unit = {
        TimeUnit.SECONDS.sleep(30)
        messageConsumer.shutdown()
      }
    }.start()

    messageConsumer.start() // handle exceptions if needs
  }
}