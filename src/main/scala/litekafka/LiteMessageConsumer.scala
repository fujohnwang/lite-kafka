package litekafka

import java.util.Collections
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import org.slf4j.LoggerFactory

import collection.JavaConversions._
import scala.beans.BeanProperty

case class MessagePosition(position: Long)

object MessagePosition {
  val earliest = new MessagePosition(kafka.api.OffsetRequest.EarliestTime)
  val latest = new MessagePosition(kafka.api.OffsetRequest.LatestTime)
}

/**
 * a MessageDispatcher will be executed in single thread by LiteMessageConsumer,
 * but it can dispatch messages concurrently or in parallel asynchronously.
 *
 * of course, if sequential message processing is enough, a MessageDispatcher can be used as message handler directly too.
 */
trait MessageDispatcher[K, V] {
  def onMessage(topic: String, key: K, message: V, partition: Int = 0): Unit
}


/**
 * A LiteMessageConsumer will ONLY consume message from partition 0 of any topic it subscribes from.
 *
 * We would like to simplify kafka's message producer/consumer model by degrading the topic to have only 1 partition.
 *
 * this LiteMessageConsumer will NOT handle failover and reconnect things, make one for yourself with the help of this class if you need.
 */
class LiteMessageConsumer[K, V](brokers: Array[String], topic: String, messageProcessor: MessageDispatcher[K, V]) {
  val logger = LoggerFactory.getLogger("LiteMessageConsumer")

  val partition: Int = 0

  @BeanProperty
  var startPosition: MessagePosition = MessagePosition.latest

  @BeanProperty
  var clientId: String = "__TRANSIENT_CLIENT_ID__"

  @BeanProperty
  var soTimeout: Int = 10000
  @BeanProperty
  var bufferSize: Int = 1024 * 64

  /**
   * if start position assigned is out of range on the broker, fails fast by throwing exception and exit.
   * recovery exceptions can be caught and handled property, as to other kind of exceptions, just let it fail fast and exit.
   */
  def start(): Unit = {
    // kick off the consumer and dispatch message received to message processor
    val metadata = findLeader()
    if (metadata == null) throw new IllegalStateException(s"Can't find metadata for Topic:$topic and Partition:0.")
    if (metadata.leader == null) throw new IllegalStateException(s"Can't find Leader for Topic:$topic and Partition:0.")

    val consumer = new SimpleConsumer(metadata.leader.host, metadata.leader.port, soTimeout, bufferSize, s"Client_${topic}_0")
    
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
}


object LiteMessageConsumer {
  def main(args: Array[String]) {
    val messageConsumer = new LiteMessageConsumer[String, String](Array("localhost:9092"), "test_topic", new MessageDispatcher[String, String] {
      override def onMessage(topic: String, key: String, message: String, partition: Int): Unit = ???
    })

    messageConsumer.start() // handle exceptions if needs
  }
}