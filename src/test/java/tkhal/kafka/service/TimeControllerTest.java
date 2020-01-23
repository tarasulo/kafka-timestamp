package tkhal.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.lang.System.getenv;

public class TimeControllerTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(TimeControllerTest.class);
    private TimeController controller;
    private KafkaServiceProducer serviceProducer;
    private Producer<String, String> producer;
    private KafkaServiceConsumer kafkaServiceConsumer;
    private KafkaConsumer<String, String> consumer;
    private static String tempMessage;

    @Test
    public void test() throws InterruptedException {
        serviceProducer = new KafkaServiceProducer();
        producer = serviceProducer.createProducer();
        String record = "First message";
        String topicRecord = "topic1";
        try {
            producer.send(new ProducerRecord<>(topicRecord, record));
        } catch (Exception e) {
            LOGGER.error("Resend failed " + e);
        }
        RunController runController = new RunController();
        runController.start();
        Thread.sleep(Integer.parseInt(getenv("DURATION"))* 1000 + 5000);

        String resultTopic = "Topic2";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(resultTopic));
        boolean condition = true;
        while (condition) {
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
            if (messages.isEmpty()) {
            } else condition = false;
            for (ConsumerRecord<String, String> message : messages) {
                try {
                    LOGGER.info("Filter controller received "
                            + message.toString());
                    tempMessage = message.value();
                } catch (Exception e) {
                    LOGGER.error(String.valueOf(e));
                }
            }
        }
        Assertions.assertEquals(record, tempMessage);
    }
}
