package litekafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.beans.BeanProperty

trait MessageSender[K, V] {
  def send(topic: String, value: V): Unit = send(topic, null.asInstanceOf[K], value)

  def send(topic: String, key: K, value: V): Unit
}


/**
 * key serializer class name and value serializer class name should be set as per the type of K and V before create() is called.
 *
 * The default type of Message sender is [String, Array[Byte]].
 */
class MessageSenderFactory[K, V](brokerList: String) {
  val brokerListPropertyName = "metadata.broker.list"
  val producerAckPropertyName = "request.required.acks"

  val keySerializerPropertyName = "key.serializer.class"
  val valueSerializerPropertyName = "serializer.class"

  protected val prop = new Properties

  @BeanProperty
  var syncMode: Int = -1
  @BeanProperty
  var messageKeySerializerClass: String = "kafka.serializer.StringEncoder"
  @BeanProperty
  var messageValueSerializerClass: String = "kafka.serializer.DefaultEncoder"

  protected var producer: Producer[K, V] = _

  def create(): MessageSender[K, V] = {
    afterPropertiesSet()
    if (producer != null) {
      return new MessageSender[K, V] {
        override def send(topic: String, key: K, value: V): Unit = producer.send(new KeyedMessage[K, V](topic, key, value))
      }
    }
    throw new IllegalStateException("messenger is not initialized yet, call afterPropertiesSet() first.")

  }

  protected def afterPropertiesSet(): Unit = {
    if (brokerList == null || brokerList.trim.length == 0) throw new IllegalArgumentException("brokerList property must be given.")
    prop.put(brokerListPropertyName, brokerList)
    prop.put(producerAckPropertyName, syncMode.toString)
    prop.put(keySerializerPropertyName, getMessageKeySerializerClass)
    prop.put(valueSerializerPropertyName, getMessageValueSerializerClass)

    producer = new Producer[K, V](new ProducerConfig(prop))
  }

  def setProperty(key: String, value: String) = prop.put(key, value)

  def destroy(): Unit = {
    if (producer != null) {
      producer.close
      producer = null
    }
  }
}

object MessageSenderFactory {
  def main(args: Array[String]) {
    val factory = new MessageSenderFactory[String, String]("192.168.1.209:9092")
    factory.setMessageKeySerializerClass("kafka.serializer.StringEncoder")
    factory.setMessageValueSerializerClass("kafka.serializer.StringEncoder")
    val sender = factory.create()

    try {
      sender.send("test", "message_key", "message")
    } finally {
      factory.destroy()
    }
  }
}