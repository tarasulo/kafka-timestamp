package tkhal.service.timeController;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.LocalTime;

public class RunKafkaConsumer extends Thread {
    private static KafkaConsumer<String, String> consumer;
    private int timestamp;

    public RunKafkaConsumer(KafkaConsumer<String, String> consumer, int timestamp) {
        this.consumer = consumer;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        while (true) {
            LocalTime timeNow = LocalTime.now();
            while (timeNow.plusSeconds(timestamp).isAfter(LocalTime.now())) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> message : messages) {
                    Storage.setPack(message.value());
                }
            }
        }
    }
}
