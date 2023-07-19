package com.ea.gamescvents;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

class ProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println(recordMetadata.toString());
        if (e != null) {
            e.printStackTrace();
        }
    }
}


public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("partitioner.class", "com.ea.gamesevents.CustomPartitioner");
        // in normal case, due to multiple retries , let's say the broker perrsisted the message in log , but returned ack which
        // did not reach the producer, producer retries again which causes "at least once" i.e. >= 1
        // to make at most once (i.e. <= 1) make retry 0, if luck is good would send once, if anything fails no retries hence max send is <=1
        // for exactly once(enable idempotence) wth max.in.flight.requests.per.connection = 1 to 5
        // this creates a producer id and mono tone increasing message id, so that in cases of retries the duplicate message is prevented.
        // this handles only retries issue and does not work if same producer instance is used across threads or multiple instances
        // In case of duplicate messages, no exception is thrown but registered as error metrics in producer metrics as well as broker metrics
        props.put("enable.idempotence", true);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        logger.info("Start sending messages...");
        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            ProducerRecord<Integer, String> record =
                    new ProducerRecord<>(AppConfigs.topicName, i, "France");
            // Headers are (key, value= any serialized object) used to define source of message needed for adding metadata that cannot be
            // added to the actual data.Used for routing messages, tracing message without parsing the data itself.
            record.headers().add("source","pos-1".getBytes(StandardCharsets.UTF_8));
            // Sync producer, that blocks entire thread
            //producer.send(new ProducerRecord<>(AppConfigs.topicName,  "Simple Message-" + i)).get();

            // Async
            producer.send(record, new ProducerCallback());
            // types of errors, retryable(leader election,connection issue) & non retryable(data serialization issue)

        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
