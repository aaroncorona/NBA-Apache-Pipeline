import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object KafkaProducerService {
  private val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)

  def produceData(key: String, value: String, topic: String): Unit = {
    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record)
    println(s"Sent message to topic $topic: key=$key, value=$value")
  }

  def closeProducer(): Unit = {
    producer.close()
    println("Producer closed.")
  }
}
