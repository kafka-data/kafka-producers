package com.ea.gamescvents;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

// Topics involved in transaction should have replication factor >= 3 & min.insync.replicas >= 2
// need to set a transaction id per instance, it internally creates implements idempotence
public class TransactionalProducer {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-id-1");
        //props.put("partitioner.class", "com.ea.gamesevents.CustomPartitioner");


        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        logger.info("Starting first transaction");
        producer.beginTransaction();
        try {
            logger.info("Start sending messages...");
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, "Simple Message-T1" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, "Simple Message-T2" + i));
            }
            logger.info("Committing first transaction");
            producer.commitTransaction();
        } catch (Exception e) {
            logger.error(" Exception in first transaction aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException();
        }

        logger.info("Starting second transaction");
        producer.beginTransaction();
        try {
            logger.info("Start sending messages...");
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, "Simple Message-T1" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, "Simple Message-T2" + i));
            }
            logger.info("Committing second transaction");
            producer.abortTransaction();
        } catch (Exception e) {
            logger.error(" Exception in second transaction aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException();
        }
        producer.close();
    }
}
