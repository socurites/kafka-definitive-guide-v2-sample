package com.socurites.kafka.sample.chap3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerEx {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerEx.class);

    public static void main(String[] args) {
        Properties kafkaPros = new Properties();
        kafkaPros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaPros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaPros)) {
            logger.info("Producer created");

            sendSyncMessage(producer);
        }
    }

    public static void sendSyncMessage(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Precision Products", "France");

        try {
            RecordMetadata recordMetadata = producer.send(record)
                    .get();

            logger.info("Record metadata: " + recordMetadata);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
