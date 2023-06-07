package content.buildon.aws.streaming.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.utils.Utils;

public class BucketPriorityAssignor extends AbstractPartitionAssignor implements Configurable {

    private AbstractPartitionAssignor fallback;
    private BucketPriorityConfig config;
    private Map<String, Bucket> buckets;
    private int lastPartitionCount;

    @Override
    public String name() {
        return "bucket-priority";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new BucketPriorityConfig(configs);
        List<Integer> bucketAlloc = new ArrayList<>(config.allocation().size());
        for (String allocItem : config.allocation()) {
            allocItem = allocItem.replaceAll("%", "").trim();
            bucketAlloc.add(Integer.parseInt(allocItem));
        }
        if (config.buckets().size() != bucketAlloc.size()) {
            throw new InvalidConfigurationException("The bucket allocation " + 
                "doesn't match with the number of buckets configured.");
        }
        int oneHundredPerc = bucketAlloc.stream()
            .mapToInt(Integer::intValue)
            .sum();
        if (oneHundredPerc != 100) {
            throw new InvalidConfigurationException("The bucket allocation " +
                "is incorrect. The sum of all buckets needs to be 100.");
        }
        try {
            fallback = config.getConfiguredInstance(
                BucketPriorityConfig.FALLBACK_ASSIGNOR_CONFIG,
                AbstractPartitionAssignor.class);
        } catch (Exception ex) {
            throw new InvalidConfigurationException("The fallback " +
                "assignor configured is invalid.", ex);
        }
        buckets = new LinkedHashMap<>();
        for (int i = 0; i < config.buckets().size(); i++) {
            String bucketName = config.buckets().get(i).trim();
            buckets.put(bucketName, new Bucket(bucketAlloc.get(i)));
        }
        // Sort the buckets with higher allocation to come
        // first than the others. This will help later during
        // the allocation if unassigned partitions are found.
        buckets = buckets.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        ByteBuffer userData = null;
        for (String topic : topics) {
            if (topic.equals(config.topic())) {
                String bucket = config.bucket();
                Charset charset = StandardCharsets.UTF_8;
                userData = charset.encode(bucket);
                break;
            }
        }
        return userData;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
        Map<String, Subscription> subscriptions) {
        int numPartitions = partitionsPerTopic.get(config.topic());
        // Check if the # of partitions has changed
        // and trigger an update if that happened.
        if (lastPartitionCount != numPartitions) {
            updatePartitionsAssignment(numPartitions);
            lastPartitionCount = numPartitions;
        }
        Map<String, List<TopicPartition>> assignments = new LinkedHashMap<>();
        Map<String, List<String>> consumersPerBucket = new LinkedHashMap<>();
        Map<String, Subscription> otherTopicsSubscriptions = new LinkedHashMap<>();
        for (String consumer : subscriptions.keySet()) {
            Subscription subscription = subscriptions.get(consumer);
            if (subscription.topics().contains(config.topic())) {
                assignments.put(consumer, new ArrayList<>());
                ByteBuffer userData = subscription.userData();
                Charset charset = StandardCharsets.UTF_8;
                String bucket = charset.decode(userData).toString();
                if (buckets.containsKey(bucket)) {
                    if (consumersPerBucket.containsKey(bucket)) {
                        List<String> consumers = consumersPerBucket.get(bucket);
                        consumers.add(consumer);
                    } else {
                        List<String> consumers = new ArrayList<>();
                        consumers.add(consumer);
                        consumersPerBucket.put(bucket, consumers);
                    }
                }
            } else {
                otherTopicsSubscriptions.put(consumer, subscription);
            }
        }
        // Evenly distribute the partitions across the
        // available consumers in a per-bucket basis.
        AtomicInteger counter = new AtomicInteger(-1);
        for (Map.Entry<String, Bucket> bucket : buckets.entrySet()) {
            List<String> consumers = consumersPerBucket.get(bucket.getKey());
            // Check if the bucket has consumers available...
            if (consumers != null && !consumers.isEmpty()) {
                for (TopicPartition partition : bucket.getValue().getPartitions()) {
                    int nextValue = counter.incrementAndGet();
                    int index = Utils.toPositive(nextValue) % consumers.size();
                    String consumer = consumers.get(index);
                    assignments.get(consumer).add(partition);
                }
            }
        }
        // If there are subscriptions for topics that
        // are not based on buckets then use the fallback
        // assignor to create their partition assignments.
        if (!otherTopicsSubscriptions.isEmpty()) {
            Map<String, List<TopicPartition>> fallbackAssignments =
                fallback.assign(partitionsPerTopic, otherTopicsSubscriptions);
            assignments.putAll(fallbackAssignments);
        }
        return assignments;
    }

    private void updatePartitionsAssignment(int numPartitions) {
        List<TopicPartition> partitions = partitions(config.topic(), numPartitions);
        if (partitions.size() < buckets.size()) {
            StringBuilder message = new StringBuilder();
            message.append("The number of partitions available for the topic '");
            message.append(config.topic()).append("' is incompatible with the ");
            message.append("number of buckets. It needs to be at least ");
            message.append(buckets.size()).append(".");
            throw new InvalidConfigurationException(message.toString());
        }
        // Sort partitions in ascendent order since
        // the partitions will be mapped into the
        // buckets from partition-0 to partition-n.
        partitions = partitions.stream()
            .sorted(Comparator.comparing(TopicPartition::partition))
            .collect(Collectors.toList());
        // Design the layout of the distribution
        int distribution = 0;
        Map<String, Integer> layout = new LinkedHashMap<>();
        for (Map.Entry<String, Bucket> entry : buckets.entrySet()) {
            int bucketSize = entry.getValue().size(partitions.size());
            layout.put(entry.getKey(), bucketSize);
            distribution += bucketSize;
        }
        // Check if there are unassigned partitions.
        // If so then distribute them over the buckets
        // starting from the top to bottom until there
        // are no partitions left.
        int remaining = partitions.size() - distribution;
        if (remaining > 0) {
            AtomicInteger counter = new AtomicInteger(-1);
            List<String> availableBuckets = new ArrayList<>();
            buckets.keySet().stream().forEach(bucket -> {
                availableBuckets.add(bucket);
            });
            while (remaining > 0) {
                int nextValue = counter.incrementAndGet();
                int index = Utils.toPositive(nextValue) % availableBuckets.size();
                String bucketName = availableBuckets.get(index);
                int bucketSize = layout.get(bucketName);
                layout.put(bucketName, ++bucketSize);
                remaining--;
            }
        }
        // Finally assign the available partitions to buckets
        int partition = -1;
        TopicPartition topicPartition = null;
        bucketAssign: for (Map.Entry<String, Bucket> entry : buckets.entrySet()) {
            int bucketSize = layout.get(entry.getKey());
            entry.getValue().getPartitions().clear();
            for (int i = 0; i < bucketSize; i++) {
                topicPartition = new TopicPartition(config.topic(), ++partition);
                entry.getValue().getPartitions().add(topicPartition);
                if (partition == partitions.size() - 1) {
                    break bucketAssign;
                }
            }
        }
    }
    
}
