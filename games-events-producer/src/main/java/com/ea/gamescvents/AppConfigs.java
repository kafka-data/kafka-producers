package com.ea.gamescvents;

public class AppConfigs {
    final static String applicationID = "HelloProducer";
    final static String bootstrapServers = "localhost:9092";
    final static String topicName = "hello-producer-topic1";
    final static int numEvents = 1000000;

    final static String topicName1 = "transaction-topic1";
    final static String topicName2 = "transaction-topic2";

    final static String kafkaConfigFileLocation = "kafka.properties";
    final static String[] eventFiles = {"data/NSE05NOV2018BHAV.csv","data/NSE06NOV2018BHAV.csv"};

    final static String topicNameThreadedProducer = "thread-topic";
}
