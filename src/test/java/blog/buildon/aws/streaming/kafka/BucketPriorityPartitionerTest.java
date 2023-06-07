package blog.buildon.aws.streaming.kafka;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BucketPriorityPartitionerTest {

    @Test
    public void checkMissingConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        try (BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner()) {
            // Check if the topic configuration is missing
            assertThrows(ConfigException.class, () -> {
                partitioner.configure(configs);
            });
            // Check if the buckets configuration is missing
            assertThrows(ConfigException.class, () -> {
                configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
                partitioner.configure(configs);
            });
            // Check if the allocation configuration is missing
            assertThrows(ConfigException.class, () -> {
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
                partitioner.configure(configs);
            });
            // Check if complete configuration is gonna be enough
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
                partitioner.configure(configs);
            });
        }
    }

    @Test
    public void checkMatchingBucketConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        try (BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner()) {
            assertThrows(InvalidConfigurationException.class, () -> {
                configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%");
                partitioner.configure(configs);
            });
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
                partitioner.configure(configs);
            });
        }
    }

    @Test
    public void checkAllocationPercentageConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        try (BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner()) {
            assertThrows(InvalidConfigurationException.class, () -> {
                configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 20%");
                partitioner.configure(configs);
            });
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
                partitioner.configure(configs);
            });
        }
    }

    @Test
    public void checkIfMinNumberPartitionsIsRespected() {
        final String topic = "test";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        // Using two buckets implies having at least two partitions...
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create a topic with only one partition...
        PartitionInfo partition0 = new PartitionInfo(topic, 0, null, null, null);
        List<PartitionInfo> partitions = List.of(partition0);
        Cluster cluster = createCluster(partitions);
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            assertThrows(InvalidConfigurationException.class, () -> {
                producer.send(new ProducerRecord<String, String>(topic, "B1-001", "value"));
            });
        }
    }

    @Test
    public void checkBucketAllocationGivenEvenAllocationConfig() {

        final String topic = "test";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2, B3");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "50%, 30%, 20%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);

        // Create 10 partitions for buckets B1, B2, and B3
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = createCluster(partitions);

        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {

            ProducerRecord<String, String> record = null;
            // Produce 10 records to the 'B1' bucket that must
            // be composed of the partitions [0, 1, 2, 3, 4]
            final AtomicInteger b1Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 0 && chosenPartition <= 4) {
                        b1Count.incrementAndGet();
                    }
                });
            }

            // Produce 10 records to the 'B2' bucket that must
            // be composed of the partitions [5, 6, 7]
            final AtomicInteger b2Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 5 && chosenPartition <= 7) {
                        b2Count.incrementAndGet();
                    }
                });
            }

            // Produce 10 records to the 'B3' bucket that must
            // be composed of the partitions [8, 9]
            final AtomicInteger b3Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B3-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 8 && chosenPartition <= 9) {
                        b3Count.incrementAndGet();
                    }
               });
            }

            // The expected output is:
            // - B1 should contain 10 records
            // - B2 should contain 10 records
            // - B3 should contain 10 records
            assertEquals(10, b1Count.get());
            assertEquals(10, b2Count.get());
            assertEquals(10, b3Count.get());

        }

    }

    @Test
    public void checkBucketAllocationGivenUnevenAllocationConfig() {

        final String topic = "test";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2, B3");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "55%, 40%, 5%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);

        // Create 10 partitions for buckets B1, B2, and B3
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = createCluster(partitions);

        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {

            ProducerRecord<String, String> record = null;
            // Produce 10 records to the 'B1' bucket that must
            // be composed of the partitions [0, 1, 2, 3, 4, 5]
            final AtomicInteger b1Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 0 && chosenPartition <= 5) {
                        b1Count.incrementAndGet();
                    }
                });
            }

            // Produce 10 records to the 'B2' bucket that must
            // be composed of the partitions [6, 7, 8, 9]
            final AtomicInteger b2Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 6 && chosenPartition <= 9) {
                        b2Count.incrementAndGet();
                    }
                });
            }

            // Produce 10 records to the 'B3' bucket that must
            // be composed of zero partitions [] because is uneven
            final AtomicInteger b3Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B3-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    // Getting a partition set to -1 means that
                    // the record didn't get written anywhere and
                    // therefore we can expect that the counter
                    // will never be incremented below.
                    if (metadata.partition() != -1) {
                        b3Count.incrementAndGet();
                    }
                });
            }

            // The expected output is:
            // - B1 should contain 10 records
            // - B2 should contain 10 records
            // - B3 should contain 0 records
            assertEquals(10, b1Count.get());
            assertEquals(10, b2Count.get());
            assertEquals(0, b3Count.get());

        }

    }

    @Test
    public void checkBucketAllocationGivenUnevenPartitionNumber() {

        final String topic = "test";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2, B3");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "55%, 40%, 5%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);

        // Create 5 partitions for buckets B1, B2, and B3
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = createCluster(partitions);

        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {

            ProducerRecord<String, String> record = null;
            // Produce 10 records to the 'B1' bucket that must
            // be composed of the partitions [0, 1, 2]
            final AtomicInteger b1Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 0 && chosenPartition <= 2) {
                        b1Count.incrementAndGet();
                    }
                });
            }

            // Produce 10 records to the 'B2' bucket that must
            // be composed of the partitions [3, 4]
            final AtomicInteger b2Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 3 && chosenPartition <= 4) {
                        b2Count.incrementAndGet();
                    }
                });
            }

            // Produce 10 records to the 'B3' bucket that must
            // be composed of zero partitions [] because is uneven
            final AtomicInteger b3Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B3-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    // Getting a partition set to -1 means that
                    // the record didn't get written anywhere and
                    // therefore we can expect that the counter
                    // will never be incremented below.
                    if (metadata.partition() != -1) {
                        b3Count.incrementAndGet();
                    }
                });
            }

            // The expected output is:
            // - B1 should contain 10 records
            // - B2 should contain 10 records
            // - B3 should contain 0 records
            assertEquals(10, b1Count.get());
            assertEquals(10, b2Count.get());
            assertEquals(0, b3Count.get());

        }

    }

    @Test
    public void checkRoundRobinBucketDataDistribution() {

        final String topic = "test";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "80%, 20%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);

        // Create 10 partitions for buckets B1 and B2 that
        // will create the following partition assignment:
        // B1 = [0, 1, 2, 3, 4, 5, 6, 7]
        // B2 = [8, 9]
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = createCluster(partitions);

        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {

            final Map<Integer, Integer> distribution = new HashMap<>();
            ProducerRecord<String, String> record = null;
            // Produce 32 records to the 'B1' bucket that
            // should distribute 4 records per partition.
            for (int i = 0; i < 32; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }

            // Produce 32 records to the 'B2' bucket that
            // should distribute 16 records per partition.
            for (int i = 0; i < 32; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }

            // The expected output is:
            // - 4 records on each partition of B1
            // - 16 records on each partition of B2
            Map<Integer, Integer> expected = new HashMap<>();
            // B1
            expected.put(0, 4);
            expected.put(1, 4);
            expected.put(2, 4);
            expected.put(3, 4);
            expected.put(4, 4);
            expected.put(5, 4);
            expected.put(6, 4);
            expected.put(7, 4);
            // B2
            expected.put(8, 16);
            expected.put(9, 16);
            assertEquals(expected, distribution);

        }

    }

    @Test
    public void checkBucketsResizeDueToPartitionsIncrease() {

        final String topic = "test";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "80%, 20%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);

        // Create 10 partitions for buckets B1 and B2 that
        // will create the following partition assignment:
        // B1 = [0, 1, 2, 3, 4, 5, 6, 7]
        // B2 = [8, 9]
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = createCluster(partitions);

        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {

            final Map<Integer, Integer> distribution = new HashMap<>();
            ProducerRecord<String, String> record = null;
            // Produce 32 records to the 'B1' bucket that
            // should distribute 4 records per partition.
            for (int i = 0; i < 32; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }

            // Produce 32 records to the 'B2' bucket that
            // should distribute 16 records per partition.
            for (int i = 0; i < 32; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }

            // The expected output is:
            // - 4 records on each partition of B1
            // - 16 records on each partition of B2
            Map<Integer, Integer> expected = new HashMap<>();
            // B1
            expected.put(0, 4);
            expected.put(1, 4);
            expected.put(2, 4);
            expected.put(3, 4);
            expected.put(4, 4);
            expected.put(5, 4);
            expected.put(6, 4);
            expected.put(7, 4);
            // B2
            expected.put(8, 16);
            expected.put(9, 16);
            assertEquals(expected, distribution);

            // Now let's force the partitions to be reallocated into
            // the buckets because the number of partitions has been
            // increased (doubled) by the user.
            increasePartitionNumber(topic, 20, producer);
            // Adding 10 more partitions will create the
            // following new partition assignment:
            // B1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
            // B2 = [16, 17, 18, 19]

            // Clear the distribution counter so we can check again
            distribution.clear();

            // Produce 32 records to the 'B1' bucket that
            // should distribute 2 records per partition.
            for (int i = 0; i < 32; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }

            // Produce 32 records to the 'B2' bucket that
            // should distribute 8 records per partition.
            for (int i = 0; i < 32; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }
            expected.clear();

            // The expected output is:
            // - 2 records on each partition of B1
            // - 8 records on each partition of B2
            // B1
            expected.put(0, 2);
            expected.put(1, 2);
            expected.put(2, 2);
            expected.put(3, 2);
            expected.put(4, 2);
            expected.put(5, 2);
            expected.put(6, 2);
            expected.put(7, 2);
            expected.put(8, 2);
            expected.put(9, 2);
            expected.put(10, 2);
            expected.put(11, 2);
            expected.put(12, 2);
            expected.put(13, 2);
            expected.put(14, 2);
            expected.put(15, 2);
            // B2
            expected.put(16, 8);
            expected.put(17, 8);
            expected.put(18, 8);
            expected.put(19, 8);
            assertEquals(expected, distribution);

        }

    }

    @SuppressWarnings("rawtypes")
    private void increasePartitionNumber(String topic, int newPartitionCount,
        MockProducer<String, String> producer) {
        List<PartitionInfo> currPartitions = producer.partitionsFor(topic);
        List<PartitionInfo> targetPartitions = new ArrayList<>(newPartitionCount);
        targetPartitions.addAll(currPartitions);
        for (int i = targetPartitions.size(); i < newPartitionCount; i++) {
            targetPartitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster newCluster = createCluster(targetPartitions);
        // Not pretty but since the MockProducer class doesn't provide
        // a way to replace the existing cluster we need to inject it
        Class mockProducerClass = MockProducer.class;
        Field clusterField = null;
        try {
            clusterField = mockProducerClass.getDeclaredField("cluster");
            clusterField.setAccessible(true);
            clusterField.set(producer, newCluster);
        } catch (Exception ex) {
        }
    }

    private Cluster createCluster(List<PartitionInfo> partitions) {
        return new Cluster("newCluster", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
    }

}
