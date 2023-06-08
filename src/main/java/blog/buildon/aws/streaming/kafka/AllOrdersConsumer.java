package blog.buildon.aws.streaming.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.ALL_ORDERS;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.createTopic;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.getConfigs;

public class AllOrdersConsumer {

    private class ConsumerThread extends Thread {

        private String threadName;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String threadName, Properties configs) {

            this.threadName = threadName;

            configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

            configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ALL_ORDERS + "-group");

            consumer = new KafkaConsumer<>(configs);
            consumer.subscribe(Arrays.asList(ALL_ORDERS));

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

    private final List<ConsumerThread> consumerThreads = new ArrayList<>();

    private void run(int numberOfThreads, Properties configs) {
        for (int i = 0; i < numberOfThreads; i++) {
            String threadName = String.format("Consumer-Thread-%d", i);
            consumerThreads.add(new ConsumerThread(threadName, configs));
        }
        consumerThreads.stream().forEach(ct -> ct.start());
    }

    public static void main(String[] args) {
        createTopic(ALL_ORDERS, 6, (short) 3);
        if (args.length >= 1) {
            int numberOfThreads = Integer.parseInt(args[0]);
            new AllOrdersConsumer().run(numberOfThreads, getConfigs());
        }
    }

}
