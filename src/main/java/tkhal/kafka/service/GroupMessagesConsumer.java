package tkhal.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class GroupMessagesConsumer {
    private final static Logger LOGGER = LoggerFactory.getLogger(GroupMessagesConsumer.class);
    private String topicName = "Topic2";
    private static KafkaServiceConsumer kafkaServiceConsumer;
    private static KafkaConsumer<String, String> consumer;

    public GroupMessagesConsumer() {
        this.kafkaServiceConsumer = new KafkaServiceConsumer();
    }

    public void run() {
        consumer = kafkaServiceConsumer.startConsumer();
        consumer.subscribe(Collections.singletonList(topicName));

        while (true) {
            // reading messages from Kafka topic
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> message : messages) {
                try {
                    LOGGER.info("GroupMessagesConsumer received "
                            + message.value());
                } catch (Exception e) {
                    LOGGER.error(String.valueOf(e));
                }
            }
        }
    }
    public static void main(String[] args) {
        GroupMessagesConsumer groupMessagesConsumer = new GroupMessagesConsumer();
        groupMessagesConsumer.run();
    }
}
