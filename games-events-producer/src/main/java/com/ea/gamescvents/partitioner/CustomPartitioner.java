package com.ea.gamescvents.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

      // When key is specified & no partitioner is specified then default partitioner works hash based mechanism.
      // when key is null round robin.But after 2.6, batching happens, a batch is filled then put to a partition before moving to next partition
      // saves bandwith number of requests

    // We need custom partitioner at times when we see that there is hot partitioning on account of frequent data
    // so that the partition accumulates all large amount of data with same key into the partition & as well as otehr data obtained via hashing.
    // custom partitioner ensures that data with particular only reaches that partition nothing else
        public void configure(Map<String, ?> configs) {}

        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, byte[] valueBytes,
                             Cluster cluster) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();

            if ((keyBytes == null) || (!(key instanceof String)))
            throw new InvalidRecordException("We expect all messages " +
                    "to have customer name as key");

            if (((String) key).equals("Banana"))
                return numPartitions - 1; // Banana will always go to last partition

            // Other records will get hashed to the rest of the partitions
            return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
        }

        public void close() {}
    }
