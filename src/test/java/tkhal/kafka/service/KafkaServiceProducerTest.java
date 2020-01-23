package tkhal.kafka.service;

import org.junit.Test;

public class KafkaServiceProducerTest {
    private KafkaServiceProducer producer;

    @Test
    public void test() {
        producer = new KafkaServiceProducer();
        producer.createProducer();
        producer.send("test");
    }
}
