package tkhal.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaServiceProducer {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaServiceProducer.class);
    private Producer<String, String> producer;

    public KafkaServiceProducer() {
    }

    public Producer<String, String> createProducer() {

        new KafkaServiceProducer();
        Properties propsRedirect = new Properties();
        propsRedirect.put("bootstrap.servers", "localhost:9092");
        propsRedirect.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsRedirect.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // creating new Kafka producer
        producer = new KafkaProducer<>(propsRedirect);
        return producer;
    }

    public void send(String record, String topicName) {
        /**
         * This is the method which sending car
         * to the topic
         */
        try {
            producer.send(new ProducerRecord<>(topicName, record));
        } catch (Exception e) {
            LOGGER.error("Resend failed " + e);
        }
    }
}
