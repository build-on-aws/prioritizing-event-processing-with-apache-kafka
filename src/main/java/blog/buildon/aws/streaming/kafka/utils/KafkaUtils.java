package blog.buildon.aws.streaming.kafka.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaUtils {

    public static final String ALL_ORDERS = "all-orders";
    public static final String ORDERS_PER_BUCKET = "orders-per-bucket";

    public static void createTopic(String topic, int numPartitions, short replicationFactor) {
        Properties configs = getConfigs();
        try (AdminClient adminClient = AdminClient.create(configs)) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> existingTopics = listTopics.names().get();
            if (!existingTopics.contains(topic)) {
                adminClient.createTopics(Collections.singleton(
                    new NewTopic(topic, numPartitions, replicationFactor)));
            }
        } catch (InterruptedException | ExecutionException ex) {
        }
    }

    private static Properties configs = new Properties();

    static {
        try {
            try (InputStream is = KafkaUtils.class.getResourceAsStream("/kafka.properties")) {
                configs.load(is);
            }
        } catch (IOException ioe) {
        }
    }

    public static Properties getConfigs() {
        return configs;
    }

}
