package tkhal.kafka.service.timeController;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tkhal.kafka.service.KafkaServiceConsumer;
import tkhal.kafka.service.KafkaServiceProducer;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;

import static java.lang.System.getenv;

public class TimeController {
    private final Logger LOGGER = LoggerFactory.getLogger(TimeController.class);
    private KafkaServiceConsumer kafkaServiceConsumer;
    private KafkaServiceProducer kafkaServiceProducer;
    private KafkaConsumer<String, String> consumer;
    private Producer<String, String> producer;
    private String consumerTopicName;
    private String producerTopicName;

    public TimeController() {
        this.kafkaServiceConsumer = new KafkaServiceConsumer();
        this.kafkaServiceProducer = new KafkaServiceProducer();
    }

    public void run() {
        consumerTopicName = "topic1";
        producerTopicName = "Topic2";
        // starting new Kafka consumer
        consumer = kafkaServiceConsumer.startConsumer(consumerTopicName);
        // starting new Kafka producer
        producer = kafkaServiceProducer.createProducer();

        int timestamp = Integer.parseInt(getenv("DURATION"));
        int timeForSend = Integer.parseInt(getenv("SEND_TIME"));
        ArrayList<String> records = new ArrayList<String>();

        while (true) {
            LocalTime timeNow = LocalTime.now();
            StringBuilder pack = new StringBuilder();
            while (timeNow.plusSeconds(timestamp).isAfter(LocalTime.now())) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> message : messages) {
                    pack.append(message.value() + " ");
                }
            }
            records.add(pack.toString());
            while (timeNow.plusSeconds(timeForSend).isAfter(LocalTime.now())) {
                for (String record : records) {
                    if (record != "" && record != " ") {
                        kafkaServiceProducer.send(record, producerTopicName);
                        LOGGER.info("TimeController resend " + record);
                    }
                    records.remove(record);
                    break;
                }
            }
        }
    }

    public static void main(String[] args) {
        TimeController timeController = new TimeController();
        timeController.run();
    }

}
