package com.socurites.kafka.sample.chap3;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerAsyncEx {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerAsyncEx.class);

    public static void main(String[] args) {
        Properties kafkaPros = new Properties();
        kafkaPros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaPros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaPros)) {
            logger.info("Producer created");

            sendAsyncMessage(producer);
        }
    }

    public static void sendAsyncMessage(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Precision Products", "France");

        producer.send(record, new DemoProducerCallback());
    }

    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (null != exception) {
                exception.printStackTrace();
            } else {
                logger.info("Record metadata: " + metadata);
            }
        }
    }
}
