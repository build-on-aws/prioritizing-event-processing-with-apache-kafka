package blog.buildon.aws.streaming.kafka;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.ALL_ORDERS;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.createTopic;
import static blog.buildon.aws.streaming.kafka.utils.KafkaUtils.getConfigs;

public class AllOrdersProducer {

    private void run(Properties configs) {

        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {

            AtomicInteger counter = new AtomicInteger(0);
            String[] buckets = {"Platinum", "Gold"};

            for (;;) {

                int value = counter.incrementAndGet();
                int index = Utils.toPositive(value) % buckets.length;
                String recordKey = buckets[index] + "-" + value;

                ProducerRecord<String, String> record =
                    new ProducerRecord<>(ALL_ORDERS, recordKey, "Value");

                producer.send(record, (metadata, exception) -> {
                    System.out.println(String.format(
                        "Record with key '%s' was sent to partition %d",
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
        createTopic(ALL_ORDERS, 6, (short) 1);
        new AllOrdersProducer().run(getConfigs());
    }

}
