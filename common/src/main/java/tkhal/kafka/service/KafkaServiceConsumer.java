package tkhal.kafka.service;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

import static java.lang.System.getenv;

public class KafkaServiceConsumer {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceConsumer.class);

    public KafkaServiceConsumer() {
    }

    public KafkaConsumer<String, String> startConsumer(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", getenv("HOST"));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", getenv("GROUP_ID"));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        LOGGER.info("Subscribed to topic " + topicName);
        return consumer;
    }
}


