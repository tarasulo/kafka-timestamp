package tkhal.service.timeController;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import tkhal.service.kafka.KafkaServiceConsumer;
import tkhal.service.kafka.KafkaServiceProducer;

import static java.lang.System.getenv;

public class TimeController {
    private KafkaServiceConsumer kafkaServiceConsumer;
    private KafkaServiceProducer kafkaServiceProducer;
    private KafkaConsumer<String, String> consumer;
    private Producer<String, String> producer;
    private String consumerTopicName;
    private String producerTopicName;

    public TimeController() {
        this.kafkaServiceConsumer = new KafkaServiceConsumer();
        this.kafkaServiceProducer = new KafkaServiceProducer();
    }

    public void run() {
        consumerTopicName = "topic0";
        producerTopicName = "Topic2";
        consumer = kafkaServiceConsumer.startConsumer(consumerTopicName);
        producer = kafkaServiceProducer.createProducer();

        int timestamp = Integer.parseInt(getenv("DURATION"));
        int timeForSend = Integer.parseInt(getenv("SEND_TIME"));
        RunKafkaConsumer runKafkaConsumer = new RunKafkaConsumer(consumer, timestamp);
        runKafkaConsumer.start();
        Thread runKafkaProducer = new Thread(new RunKafkaProducer(kafkaServiceProducer, timeForSend, producerTopicName));
        runKafkaProducer.start();
    }

    public static void main(String[] args) {
        TimeController timeController = new TimeController();
        timeController.run();
    }

}
