package blog.buildon.aws.streaming.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

public class Bucket implements Comparable<Bucket> {

    private int allocation;
    private List<TopicPartition> partitions;
    private AtomicInteger counter;

    public Bucket(int allocation) {
        this.allocation = allocation;
        partitions = new ArrayList<>();
        counter = new AtomicInteger(-1);
    }

    public int nextPartition() {
        if (!partitions.isEmpty()) {
            int nextValue = counter.incrementAndGet();
            int index = Utils.toPositive(nextValue) % partitions.size();
            return partitions.get(index).partition();
        }
        return -1;
    }

    @Override
    public int compareTo(Bucket bucket) {
        int result = 0;
        if (getAllocation() < bucket.getAllocation()) {
            result = 1;
        } else if (getAllocation() > bucket.getAllocation()) {
            result = -1;
        }
        return result;
    }

    public void decrementCounter() {
        counter.decrementAndGet();
    }

    public int size(int numPartitions) {
        return Math.round(((float) allocation / 100) * numPartitions);
    }

    public int getAllocation() {
        return allocation;
    }

    public List<TopicPartition> getPartitions() {
        return partitions;
    }

}
