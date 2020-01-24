package tkhal.kafka.service;

import org.junit.Test;

public class KafkaServiceConsumerTest {
    KafkaServiceConsumer consumer;

    @Test
    public void test() {
        consumer = new KafkaServiceConsumer();
        consumer.startConsumer("topic1");
    }
}
