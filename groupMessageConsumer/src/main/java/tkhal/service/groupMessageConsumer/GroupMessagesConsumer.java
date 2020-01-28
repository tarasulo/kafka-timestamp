package tkhal.service.groupMessageConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tkhal.service.kafka.KafkaServiceConsumer;

import java.time.Duration;

public class GroupMessagesConsumer {
    private final Logger LOGGER = LoggerFactory.getLogger(GroupMessagesConsumer.class);
    private String topicName;
    private KafkaServiceConsumer kafkaServiceConsumer;
    private KafkaConsumer<String, String> consumer;

    public GroupMessagesConsumer(String topicName) {
        this.kafkaServiceConsumer = new KafkaServiceConsumer();
        this.topicName = topicName;
    }

    public void run() {
        consumer = kafkaServiceConsumer.startConsumer(topicName);

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
        GroupMessagesConsumer groupMessagesConsumer = new GroupMessagesConsumer("Topic2");
        groupMessagesConsumer.run();
    }
}
