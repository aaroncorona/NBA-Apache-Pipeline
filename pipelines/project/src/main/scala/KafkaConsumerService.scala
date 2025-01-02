import org.apache.kafka.clients.consumer.{KafkaConsumer}
import java.util.{Properties, Collections}

object KafkaConsumerService {
  private val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")  // Kafka server
  props.put("group.id", "nba-consumer-group")  // Consumer group ID
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")

  private val consumer = new KafkaConsumer[String, String](props)

  def consumeData(topic: String): Unit = {
    consumer.subscribe(Collections.singletonList(topic))

    while (true) {
      val records = consumer.poll(1000)
      records.forEach(record => {
        println(s"Received message: key=${record.key()}, value=${record.value()}")
      })
    }
  }

  def closeConsumer(): Unit = {
    consumer.close()
    println("Consumer closed.")
  }
}
