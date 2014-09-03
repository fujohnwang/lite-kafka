package litekafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.beans.BeanProperty

trait TextMessenger {
  def send(topic: String, message: String): Unit

  def send(topic: String, messageKey: String, message: String): Unit
}

class KafkaTextMessenger(producer: Producer[String, String]) extends TextMessenger {
  override def send(topic: String, message: String): Unit = send(topic, null, message)

  override def send(topic: String, messageKey: String, messageBody: String): Unit = {
    val message = new KeyedMessage[String, String](topic, messageKey, messageBody)
    producer.send(message)
  }
}


/**
 * If a more fine-tuned Kafka Producer is needed, subclass this one to make yourself at ease.
 */
class KafkaTextMessengerFactory {
  val brokerListPropertyName = "metadata.broker.list"
  val producerAckPropertyName = "request.required.acks"

  val keySerializerPropertyName = "key.serializer.class"
  val valueSerializerPropertyName = "serializer.class"

  @BeanProperty
  var syncMode: Int = -1
  @BeanProperty
  var brokerList: String = _
  @BeanProperty
  var messageKeySerializerClass: String = "kafka.serializer.StringEncoder"
  @BeanProperty
  var messageValueSerializerClass: String = "kafka.serializer.StringEncoder"

  protected var messenger: TextMessenger = _
  protected var producer: Producer[String, String] = _

  def create: TextMessenger = {
    afterPropertiesSet()
    if (messenger != null) messenger else throw new IllegalStateException("messenger is not initialized yet, call afterPropertiesSet() first.")
  }

  def afterPropertiesSet(): Unit = {
    if (brokerList == null || brokerList.trim.length == 0) throw new IllegalArgumentException("brokerList property must be given.")
    val prop = new Properties
    prop.put(brokerListPropertyName, brokerList)
    prop.put(producerAckPropertyName, syncMode.toString)
    prop.put(keySerializerPropertyName, getMessageKeySerializerClass)
    prop.put(valueSerializerPropertyName, getMessageValueSerializerClass)

    producer = new Producer[String, String](new ProducerConfig(prop))
    messenger = new KafkaTextMessenger(producer)
  }

  def destroy(): Unit = {
    if (producer != null) {
      producer.close
      producer = null
    }
  }
}


/**
 * 因为我们发送的KeyedMessage， Key和Value都是String，所以，原则上需要为Key和Value都指定序列化类，
 * 但如果不指定key的序列化类，默认会使用Value的，所以，我们只要指定Value的序列化类即可。
 */
object KafkaTextMessenger {
  def main(args: Array[String]) {
    val topic = "csw_nbk_data"
    val brokerList = "192.168.1.209:9092"

    val messengerFactory = new KafkaTextMessengerFactory()
    messengerFactory.setBrokerList(brokerList)
    messengerFactory.afterPropertiesSet()
    val producer = messengerFactory.create

    try {
      producer.send(topic, "12345", "马来西亚2")
    } finally {
      messengerFactory.destroy()
    }
  }
}
