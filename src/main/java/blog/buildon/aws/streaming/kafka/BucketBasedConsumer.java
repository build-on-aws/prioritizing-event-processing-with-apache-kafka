package blog.buildon.aws.streaming.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import code.buildon.aws.streaming.kafka.BucketPriorityAssignor;
import code.buildon.aws.streaming.kafka.BucketPriorityConfig;

import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.ORDERS_PER_BUCKET;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.createTopic;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.getConfigs;

public class BucketBasedConsumer {

    private class ConsumerThread implements Runnable {

        private String threadName;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String bucketName,
            String threadName, Properties configs) {

            this.threadName = threadName;

            configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

            configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ORDERS_PER_BUCKET + "-group");

            // Configuring the bucket priority pattern

            configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                BucketPriorityAssignor.class.getName());

            configs.put(BucketPriorityConfig.TOPIC_CONFIG, ORDERS_PER_BUCKET);
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
            configs.put(BucketPriorityConfig.BUCKET_CONFIG, bucketName);

            consumer = new KafkaConsumer<>(configs);
            consumer.subscribe(Arrays.asList(ORDERS_PER_BUCKET));

        }

        @Override
        public void run() {
            for (;;) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofSeconds(Integer.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("[%s] Key = %s, Partition = %d",
                        threadName, record.key(), record.partition()));
                }
            }
        }

    }

    private void run(String bucketName, int numberOfThreads, Properties configs) {
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            String threadName = String.format("Consumer-Thread-%d", i);
            executorService.submit(new ConsumerThread(bucketName, threadName, configs));
        }
    }

    public static void main(String[] args) {
        createTopic(ORDERS_PER_BUCKET, 6, (short)1);
        if (args.length >= 2) {
            String bucketName = args[0];
            int numberOfThreads = Integer.parseInt(args[1]);
            new BucketBasedConsumer().run(bucketName,
                numberOfThreads, getConfigs());
        }
    }

}
