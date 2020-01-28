package tkhal.service.timeController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tkhal.service.kafka.KafkaServiceProducer;

import java.time.LocalTime;
import java.util.LinkedList;

public class RunKafkaProducer implements Runnable {

    private KafkaServiceProducer kafkaServiceProducer;
    private final Logger LOGGER = LoggerFactory.getLogger(TimeController.class);
    private int timeForSend;
    private String producerTopicName;

    public RunKafkaProducer(KafkaServiceProducer kafkaServiceProducer, int timeForSend, String producerTopicName) {
        this.kafkaServiceProducer = kafkaServiceProducer;
        this.timeForSend = timeForSend;
        this.producerTopicName = producerTopicName;
    }

    @Override
    public void run() {
        LinkedList<String> records = new LinkedList<>();
        while (true) {
            if (Storage.getPack().toString().hashCode() != 0) {
                String buffer = Storage.getPack().toString();
                records.add(buffer);
                LocalTime timeNow = LocalTime.now();
                while (timeNow.plusSeconds(timeForSend).isAfter(LocalTime.now())) {
                    for (String record : records) {
                        if (record.hashCode() != 0) {
                            kafkaServiceProducer.send(record, producerTopicName);
                            LOGGER.info("TimeController resend " + record);
                        }
                        records.remove(record);
                        break;
                    }
                }
                Storage.clear(buffer);
            }
        }
    }
}
