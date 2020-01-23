package tkhal.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;

import static java.lang.System.getenv;

public class TimeController {
    private final static Logger LOGGER = LoggerFactory.getLogger(TimeController.class);
    private static KafkaServiceConsumer kafkaServiceConsumer;
    private static KafkaServiceProducer kafkaServiceProducer;
    private static KafkaConsumer<String, String> consumer;
    private static Producer<String, String> producer;
    private static String tempMessage;

    public TimeController() {
        this.kafkaServiceConsumer = new KafkaServiceConsumer();
        this.kafkaServiceProducer = new KafkaServiceProducer();
    }

    public void run() throws InterruptedException {
        // starting new Kafka consumer
        consumer = kafkaServiceConsumer.startConsumer();
        // starting new Kafka producer
        producer = kafkaServiceProducer.createProducer();

        int timestamp = Integer.parseInt(getenv("DURATION"));

        while (true) {
            LocalTime timeNow = LocalTime.now();
            ArrayList<String> records = new ArrayList<String>();

            while (timeNow.plusSeconds(timestamp).isAfter(LocalTime.now())) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
                if (messages.isEmpty()) {
                } else {
                    for (ConsumerRecord<String, String> message : messages) {
                        records.add(message.value());
                    }
                }
            }

            if (records.size() > 0) {
                for (String record : records) {
                    kafkaServiceProducer.send(record);
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TimeController timeController = new TimeController();
        timeController.run();
    }
}
