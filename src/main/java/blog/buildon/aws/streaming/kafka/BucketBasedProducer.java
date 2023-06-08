package blog.buildon.aws.streaming.kafka;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import code.buildon.aws.streaming.kafka.BucketPriorityConfig;
import code.buildon.aws.streaming.kafka.BucketPriorityPartitioner;

import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.ORDERS_PER_BUCKET;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.createTopic;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.getConfigs;

public class BucketBasedProducer {

    private void run(Properties configs) {

        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // Configuring the bucket priority pattern

        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
            BucketPriorityPartitioner.class.getName());

        configs.put(BucketPriorityConfig.TOPIC_CONFIG, ORDERS_PER_BUCKET);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {

            AtomicInteger counter = new AtomicInteger(0);
            String[] buckets = {"Platinum", "Gold"};

            for (;;) {

                int value = counter.incrementAndGet();
                int index = Utils.toPositive(value) % buckets.length;
                String recordKey = buckets[index] + "-" + value;

                ProducerRecord<String, String> record =
                    new ProducerRecord<>(ORDERS_PER_BUCKET, recordKey, "Value");

                producer.send(record, (metadata, exception) -> {
                    System.out.println(String.format(
                        "Key '%s' was sent to partition %d",
                        recordKey, metadata.partition()));
                });

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }

            }

        }

    }

    public static void main(String[] args) {
        createTopic(ORDERS_PER_BUCKET, 6, (short)1);
        new BucketBasedProducer().run(getConfigs());
    }

}
