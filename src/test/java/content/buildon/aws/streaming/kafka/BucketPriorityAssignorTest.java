package content.buildon.aws.streaming.kafka;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BucketPriorityAssignorTest {

    @Test
    public void checkMissingConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        final BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        // Check if the topic configuration is missing
        assertThrows(ConfigException.class, () -> {
            assignor.configure(configs);
        });
        // Check if the buckets configuration is missing
        assertThrows(ConfigException.class, () -> {
            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
            assignor.configure(configs);
        });
        // Check if the allocation configuration is missing
        assertThrows(ConfigException.class, () -> {
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
            assignor.configure(configs);
        });
        // Check if complete configuration is gonna be enough
        assertDoesNotThrow(() -> {
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
            assignor.configure(configs);
        });
    }

    @Test
    public void checkMatchingBucketConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        final BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assertThrows(InvalidConfigurationException.class, () -> {
            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%");
            assignor.configure(configs);
        });
        assertDoesNotThrow(() -> {
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
            assignor.configure(configs);
        });
    }

    @Test
    public void checkAllocationPercentageConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        final BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assertThrows(InvalidConfigurationException.class, () -> {
            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 20%");
            assignor.configure(configs);
        });
        assertDoesNotThrow(() -> {
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
            assignor.configure(configs);
        });
    }

    @Test
    public void checkIfFallbackPartitionerIsValid() {
        final Map<String, String> configs = new HashMap<>();
        final BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assertThrows(InvalidConfigurationException.class, () -> {
            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
            configs.put(BucketPriorityConfig.FALLBACK_ASSIGNOR_CONFIG,
                BucketPriorityAssignorTest.class.getName());
            assignor.configure(configs);
        });
    }

    @Test
    public void checkIfMinNumberPartitionsIsRespected() {
        final String topic = "test";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        // Using two buckets implies having at least two partitions...
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
        configs.put(BucketPriorityConfig.BUCKET_CONFIG, "B1");
        BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assignor.configure(configs);
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, Integer> partitionsPerTopic = Map.of(topic, 1);
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions =
                Map.of("consumer-0", new ConsumerPartitionAssignor.Subscription(List.of(topic)));
            assignor.assign(partitionsPerTopic, subscriptions);
        });
    }

    @Test
    public void checkMultipleTopicsAssignment() {

        final String regularTopic = "regularTopic";
        final String bucketTopic = "bucketTopic";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, bucketTopic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
        BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assignor.configure(configs);

        // Create the partitions and the subscriptions
        Map<String, Integer> partitionsPerTopic = Map.of(regularTopic, 6, bucketTopic, 6);
        Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();

        // Create 4 consumers, 2 for each topic
        int count = 0;
        for (int i = 0; i < 2; i++) {
            subscriptions.put(String.format("consumer-%d", count++),
                new ConsumerPartitionAssignor.Subscription(
                    List.of(regularTopic)));
        }
        for (int i = 0; i < 2; i++) {
            subscriptions.put(String.format("consumer-%d", count++),
                new ConsumerPartitionAssignor.Subscription(
                    List.of(bucketTopic), StandardCharsets.UTF_8.encode("B1")));
        }

        // Execute the assignor
        Map<String, List<TopicPartition>> assignments =
            assignor.assign(partitionsPerTopic, subscriptions);

        // The expected output is that each of the 4 consumers
        // will have assignments and their assignments need to
        // be greather than zero.
        assertEquals(4, assignments.size());
        assignments.values().forEach(v -> assertTrue(v.size() > 0));

    }

    @Test
    public void checkPerBucketAssignmentWithoutRebalance() {

        final String bucketTopic = "bucketTopic";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, bucketTopic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "80%, 20%");
        BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assignor.configure(configs);

        // Create the partitions and the subscriptions
        Map<String, Integer> partitionsPerTopic = Map.of(bucketTopic, 10);
        Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();

        int count = 0;
        // Create 8 consumers for the B1 bucket
        for (int i = 0; i < 8; i++) {
            subscriptions.put(String.format("consumer-%d", count++),
                new ConsumerPartitionAssignor.Subscription(
                    List.of(bucketTopic), StandardCharsets.UTF_8.encode("B1")));
        }
        // Create 2 consumers for the B2 bucket
        for (int i = 0; i < 2; i++) {
            subscriptions.put(String.format("consumer-%d", count++),
                new ConsumerPartitionAssignor.Subscription(
                    List.of(bucketTopic), StandardCharsets.UTF_8.encode("B2")));
        }

        // Execute the assignor
        Map<String, List<TopicPartition>> assignments =
            assignor.assign(partitionsPerTopic, subscriptions);

        // The expected output is that each of the 10 consumers
        // will have assignments and their assignments need to
        // be greather than zero.
        assertEquals(10, assignments.size());
        assignments.values().forEach(v -> assertTrue(v.size() > 0));

        // Also consumer-0 to consumer-7 should be working
        // on B1 while consumer-8 and consumer-9 should be
        // working on B2.
        final Set<String> b1Consumers = new TreeSet<>();
        final Set<String> b2Consumers = new TreeSet<>();
        assignments.entrySet().forEach(assignment -> {
            String consumer = assignment.getKey();
            assignment.getValue().stream().forEach(tp -> {
                if (tp.partition() >= 0 && tp.partition() <= 7) { // B1
                    b1Consumers.add(consumer);
                }
                if (tp.partition() >= 8 && tp.partition() <= 9) { // B2
                    b2Consumers.add(consumer);
                }
            });
        });
        count = 0;
        Set<String> expectedB1Consumers = new TreeSet<>();
        for (int i = 0; i < 8; i++) {
            expectedB1Consumers.add(String.format("consumer-%d", count++));
        }
        Set<String> expectedB2Consumers = new TreeSet<>();
        for (int i = 0; i < 2; i++) {
            expectedB2Consumers.add(String.format("consumer-%d", count++));
        }

        // Check if the expected consumers is correct
        assertEquals(expectedB1Consumers, b1Consumers);
        assertEquals(expectedB2Consumers, b2Consumers);

    }

    @Test
    public void checkPerBucketAssignmentWithRebalance() {

        final String bucketTopic = "bucketTopic";
        final Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, bucketTopic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "80%, 20%");
        BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assignor.configure(configs);

        // Create the partitions and the subscriptions
        Map<String, Integer> partitionsPerTopic = Map.of(bucketTopic, 10);
        Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();

        int count = 0;
        // Create 8 consumers for the B1 bucket
        for (int i = 0; i < 8; i++) {
            subscriptions.put(String.format("consumer-%d", count++),
                new ConsumerPartitionAssignor.Subscription(
                    List.of(bucketTopic), StandardCharsets.UTF_8.encode("B1")));
        }
        // Create 2 consumers for the B2 bucket
        for (int i = 0; i < 2; i++) {
            subscriptions.put(String.format("consumer-%d", count++),
                new ConsumerPartitionAssignor.Subscription(
                    List.of(bucketTopic), StandardCharsets.UTF_8.encode("B2")));
        }

        // Execute the assignor
        Map<String, List<TopicPartition>> assignments =
            assignor.assign(partitionsPerTopic, subscriptions);

        // The expected output is that each of the 10 consumers
        // will have assignments and their assignments need to
        // be greather than zero.
        assertEquals(10, assignments.size());
        assignments.values().forEach(v -> assertTrue(v.size() > 0));

        // Also consumer-0 to consumer-7 should be working
        // on B1 while consumer-8 and consumer-9 should be
        // working on B2.
        final Set<String> b1Consumers = new TreeSet<>();
        final Set<String> b2Consumers = new TreeSet<>();
        assignments.entrySet().forEach(assignment -> {
            String consumer = assignment.getKey();
            assignment.getValue().stream().forEach(tp -> {
                if (tp.partition() >= 0 && tp.partition() <= 7) { // B1
                    b1Consumers.add(consumer);
                }
                if (tp.partition() >= 8 && tp.partition() <= 9) { // B2
                    b2Consumers.add(consumer);
                }
            });
        });
        count = 0;
        Set<String> expectedB1Consumers = new TreeSet<>();
        for (int i = 0; i < 8; i++) {
            expectedB1Consumers.add(String.format("consumer-%d", count++));
        }
        Set<String> expectedB2Consumers = new TreeSet<>();
        for (int i = 0; i < 2; i++) {
            expectedB2Consumers.add(String.format("consumer-%d", count++));
        }

        // Check if the expected consumers is correct
        assertEquals(expectedB1Consumers, b1Consumers);
        assertEquals(expectedB2Consumers, b2Consumers);

        // ***************** Rebalance *****************

        // Now let's simulate a rebalance triggered because two
        // consumers from each bucket presumably dropped.
        subscriptions.clear();
        count = 0;
        // Create 6 consumers only for the B1 bucket
        for (int i = 0; i < 6; i++) {
            subscriptions.put(String.format("consumer-%d", count++),
                new ConsumerPartitionAssignor.Subscription(
                    List.of(bucketTopic), StandardCharsets.UTF_8.encode("B1")));
        }
        // Zero consumers will be created for B2

        // Execute the assignor one more time...
        assignments = assignor.assign(partitionsPerTopic, subscriptions);

        // The expected output is that now we have only 6 consumers
        // and each one of them still need to have their assignments
        // and their assignments need to be greather than zero.
        assertEquals(6, assignments.size());
        assignments.values().forEach(v -> assertTrue(v.size() > 0));

        // Also consumer-0 to consumer-5 should be working
        // on B1 while B2 should have zero consumers...
        b1Consumers.clear();
        b2Consumers.clear();
        assignments.entrySet().forEach(assignment -> {
            String consumer = assignment.getKey();
            assignment.getValue().stream().forEach(tp -> {
                if (tp.partition() >= 0 && tp.partition() <= 7) { // B1
                    b1Consumers.add(consumer);
                }
                if (tp.partition() >= 8 && tp.partition() <= 9) { // B2
                    b2Consumers.add(consumer);
                }
            });
        });
        count = 0;
        expectedB1Consumers.clear();
        for (int i = 0; i < 6; i++) {
            expectedB1Consumers.add(String.format("consumer-%d", count++));
        }
        expectedB2Consumers.clear();

        // Check if the expected consumers is correct
        assertEquals(expectedB1Consumers, b1Consumers);
        assertEquals(expectedB2Consumers, b2Consumers);

    }
    
}
