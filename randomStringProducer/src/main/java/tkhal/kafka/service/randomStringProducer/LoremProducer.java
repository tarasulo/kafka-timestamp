package tkhal.kafka.service.randomStringProducer;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tkhal.kafka.service.KafkaServiceProducer;

import java.time.LocalTime;

public class LoremProducer {
    private final Logger LOGGER = LoggerFactory.getLogger(LoremProducer.class);
    private String topicName;
    private KafkaServiceProducer kafkaServiceProducer;
    private Producer<String, String> producer;
    private String generatedString;
    private LocalTime localTime;

    public LoremProducer(String topicName) {
        this.kafkaServiceProducer = new KafkaServiceProducer();
        this.topicName = topicName;
    }

    public void run() throws InterruptedException {
        producer = kafkaServiceProducer.createProducer();
        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        localTime = LocalTime.now();
        for (LocalTime i=localTime; i.isBefore(localTime.plusSeconds(40)); i=LocalTime.now()) {
            generatedString = RandomStringUtils.random(length, useLetters, useNumbers);
            Thread.sleep(300L);
            LOGGER.info(generatedString);
            try {
                kafkaServiceProducer.send(generatedString, topicName);
            } catch (Exception e) {
                LOGGER.error("Resend failed " + e);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        LoremProducer loremProducer = new LoremProducer("topic1");
        loremProducer.run();
    }
}
