package code.buildon.aws.streaming.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class BucketPriorityConfig extends AbstractConfig {

    public BucketPriorityConfig(Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public String topic() {
        return getString(TOPIC_CONFIG);
    }

    public List<String> buckets() {
        return getList(BUCKETS_CONFIG);
    }

    public List<String> allocation() {
        return getList(ALLOCATION_CONFIG);
    }

    public String bucket() {
        return getString(BUCKET_CONFIG);
    }

    public String delimiter() {
        return getString(DELIMITER_CONFIG);
    }

    public String fallbackAssignor() {
        return getString(FALLBACK_ASSIGNOR_CONFIG);
    }

    private static final ConfigDef CONFIG;
    
    public static final String TOPIC_CONFIG = "bucket.priority.topic";
    public static final String TOPIC_CONFIG_DOC = "Which topic should have its partitions mapped to buckets.";
    public static final String BUCKETS_CONFIG = "bucket.priority.buckets";
    public static final String BUCKETS_CONFIG_DOC = "List of the bucket names.";
    public static final String BUCKET_CONFIG = "bucket.priority.bucket";
    public static final String BUCKET_CONFIG_DOC = "Bucket that the consumer will be assigned to.";
    public static final String BUCKET_CONFIG_DEFAULT = "";
    public static final String DELIMITER_CONFIG = "bucket.priority.delimiter";
    public static final String DELIMITER_CONFIG_DOC = "Delimiter used to look up the bucket name in the key.";
    public static final String DELIMITER_CONFIG_DEFAULT = "-";
    public static final String ALLOCATION_CONFIG = "bucket.priority.allocation";
    public static final String ALLOCATION_CONFIG_DOC = "Allocation in percentage for each bucket.";
    public static final String FALLBACK_ASSIGNOR_CONFIG = "bucket.priority.fallback.assignor";
    public static final String FALLBACK_ASSIGNOR_CONFIG_DOC = "Which assignor to use as fallback strategy.";
    public static final String FALLBACK_ASSIGNOR_CONFIG_DEFAULT = RangeAssignor.class.getName();

    static {
        CONFIG = new ConfigDef()
            .define(TOPIC_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                TOPIC_CONFIG_DOC)
            .define(BUCKETS_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                BUCKETS_CONFIG_DOC)
            .define(ALLOCATION_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                ALLOCATION_CONFIG_DOC)
            .define(BUCKET_CONFIG,
                ConfigDef.Type.STRING,
                BUCKET_CONFIG_DEFAULT,
                ConfigDef.Importance.HIGH,
                BUCKET_CONFIG_DOC)
            .define(DELIMITER_CONFIG,
                ConfigDef.Type.STRING,
                DELIMITER_CONFIG_DEFAULT,
                ConfigDef.Importance.LOW,
                DELIMITER_CONFIG_DOC)
            .define(
                FALLBACK_ASSIGNOR_CONFIG,
                ConfigDef.Type.CLASS,
                FALLBACK_ASSIGNOR_CONFIG_DEFAULT,
                ConfigDef.Importance.LOW,
                FALLBACK_ASSIGNOR_CONFIG_DOC);
    }

}
