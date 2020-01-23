package tkhal.kafka.service;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;

public class LoremProducer {
    private final static Logger LOGGER = LoggerFactory.getLogger(LoremProducer.class);
    private String topicName = "topic1";
    private static KafkaServiceProducer kafkaServiceProducer;
    private static Producer<String, String> producer;
    private String generatedString;
    private LocalTime localTime;

    public LoremProducer() {
        this.kafkaServiceProducer = new KafkaServiceProducer();
    }

    public void run() throws InterruptedException {
        producer = kafkaServiceProducer.createProducer();

        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        localTime = LocalTime.now();
        for (LocalTime i=localTime; i.isBefore(localTime.plusSeconds(2)); i=LocalTime.now()) {
            generatedString = RandomStringUtils.random(length, useLetters, useNumbers);
            Thread.sleep(300L);
            System.out.println(generatedString);
            try {
                producer.send(new ProducerRecord<>(topicName, generatedString));
            } catch (Exception e) {
                LOGGER.error("Resend failed " + e);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        LoremProducer loremProducer = new LoremProducer();
        loremProducer.run();
    }
}
