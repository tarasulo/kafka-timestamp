package tkhal.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

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

        long duration = Long.parseLong(getenv("DURATION"));

        while (true) {
            // reading messages from Kafka topic
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(duration));
            for (ConsumerRecord<String, String> message : messages) {
                try {
                    LOGGER.info("Controller received "
                            + message.toString());
                    tempMessage = message.value();
                } catch (Exception e) {
                    LOGGER.error(String.valueOf(e));
                }
                // sending car by Kafka producer
                kafkaServiceProducer.send(tempMessage);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TimeController timeController = new TimeController();
        timeController.run();
    }
}
