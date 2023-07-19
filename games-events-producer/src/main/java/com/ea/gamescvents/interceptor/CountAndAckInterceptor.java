package com.ea.gamescvents.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CountAndAckInterceptor implements ProducerInterceptor {

    ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor();
    static AtomicLong numSent = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);

    public void configure(Map<String, ?> map) {
        // Gets the producer config here
        Long windowSize = Long.valueOf(
                (String) map.get("counting.interceptor.window.size.ms"));
        executorService.scheduleAtFixedRate(CountAndAckInterceptor::run,
                windowSize, windowSize, TimeUnit.MILLISECONDS);
    }
    // Invoked before the message is even serialized, can intercept & modify the record
    // Extract sensitive info, enriching enhancing the message,adding header, capturing monitoring tracing info, redacting sensitive info
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        numSent.incrementAndGet();
        return producerRecord;
    }

    // After the ack is received
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        numAcked.incrementAndGet();
    }

    public void close() {
        executorService.shutdownNow();
    }

    public static void run() {
        System.out.println(numSent.getAndSet(0));
        System.out.println(numAcked.getAndSet(0));
    }
    // Usage
    /*
    Add your jar to the classpath:
export CLASSPATH=$CLASSPATH:~./target/CountProducerInterceptor-1.0-SNAPSHOT.jar
Create a config file that includes:
interceptor.classes=com.shapira.examples.interceptors.CountProducerInterceptor counting.interceptor.window.size.ms=10000
Run the application as you normally would, but make sure to include the configuration that you created in the previous step:
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic interceptor-test --producer.config producer.config


     */

    // resources quotas & limit at producer consumer broker level allow to put limits on message byte/s as well as % of cpu processing time
    // on the records
}
