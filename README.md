# Prioritizing Event Processing with Apache Kafka

Implement event processing prioritization in [Apache Kafka](https://kafka.apache.org) is often a hard task because Kafka doesn't support broker-level reordering of messages like some messaging technologies do. This is not necessarily a limitation, since Kafka is a [distributed commit log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying). With this data structure, messages are immutable, and so their ordering is within partitions. But this doesn't change the fact the developers may need to implement event processing prioritization with Kafka, anyway.

This project addresses event processing prioritization via the bucket pattern. It groups partitions into simpler abstractions called buckets. Bigger buckets mean a higher priority, and smaller buckets mean less priority. The number of partitions associated with each bucket defines their size. The bucket pattern also addresses code simplicity by providing a way to do all of this without forcing developers to handle low-level code related to event partitioning and consumer assignment.

Let's understand how this works with an example.

![Partitioner Overview](images/partitioner-overview.png)

Here we can see that the partitions were grouped into the buckets `Platinum` and `Gold`. The Platinum bucket has a higher priority and therefore was configured to have `70%` of the allocation, whereas the Gold bucket has lower priority and therefore was configured to have only `30%`. This means that for a topic that contains `6` partitions, `4` of them will be associated with the Platinum bucket and `2` will be associated with the Gold bucket. To implement the prioritization, there has to be a process that ensures that messages with higher priority will end up in one the partitions from the Platinum bucket and messages with lower priority will end up in one the partitions from the Gold bucket. Consumers need to subscribe to the topic knowing which buckets they need to be associated with. This means that developers can decide to execute more consumers for the Platinum bucket and fewer consumers for the Gold bucket to ensure that they process high priority messages faster.

To ensure that each message will end up in their respective bucket, use the `BucketPriorityPartitioner`. This partitioner uses data in the message key to decide which bucket to use and therefore which partition from the bucket the message should be written. This partitioner distributes the messages within the bucket using a round robin algorithm to maximize consumption parallelism. On the consumer side, use the `BucketPriorityAssignor` to ensure that the consumer will be assigned only to the partitions that represent the bucket they want to process.

![Assignor Overview](images/assignor-overview.png)

With the bucket priority, you can implement event processing prioritization by having more consumers working on buckets with higher priorities, while buckets with less priority can have fewer consumers. Event processing prioritization can also be obtained by executing these consumers in an order that gives preference to processing high priority buckets before the less priority ones. While coordinating this execution may involve some extra coding from you (perhaps using some sort of scheduler) you don't have to implement low-level code to manage partition assignment and keep your consumers simple by leveraging the standard `subscribe()` and `poll()` methods.

You can read more about the bucket priority pattern in this blog post: https://www.buildon.aws/posts/prioritizing-event-processing-with-apache-kafka

## Building the project

The first thing you need to do to start using this partitioner is building it. In order to do that, you need to install the following dependencies:

- [Java 11+](https://openjdk.java.net/)
- [Apache Maven](https://maven.apache.org/)

After installing these dependencies, execute the following command:

```bash
mvn clean package
```

## Using the partitioner

To use the `BucketPriorityPartitioner` in your producer you need to register it in the configuration.

```bash
Properties configs = new Properties();

configs.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,
   BucketPriorityPartitioner.class.getName());

KafkaProducer<K, V> producer = new KafkaProducer<>(configs);
```

To work properly, you need to specify in the configuration which topic will have its partitions grouped into buckets. This is important because, in Kafka, topics are specified at a message level and not at a producer level. This means that the same producer can write messages on different topics, so the partitioner needs to know which topic will have their partitions grouped into buckets.

```bash
configs.setProperty(BucketPriorityConfig.TOPIC_CONFIG, "orders");
```

Finally, specify in the configuration which buckets will be configured and what is the partition allocation for each one of them. The partition allocation is specified in terms of percentage. Note that the usage of the symbol `%` is optional.

```bash
configs.setProperty(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
configs.setProperty(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
```

The partitioner ensures that all partitions from the topic will be assigned to the buckets.
In case of the allocation result in some partitions being left behind because the distribution is not even, the remaining partitions will be assigned to the buckets using a round robin algorithm over the buckets sorted by allocation.

### Messages and buckets

In order to specify which bucket should be used, your producer need to provide this information on the message key. The partitioner will inspect each key in the attempt to understand in which bucket the message should be written. For this reason, the key must be an instance of a [java.lang.String](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/String.html) and it needs to contain the bucket name either as one literal string or as the first part of a string separated by a delimiter. For example, to specify that the bucket is `Platinum` then following examples are valid:

* Key = `"Platinum"`
* Key = `"Platinum-001"`
* Key = `"Platinum-Group01-001"`

The default delimiter is `-` but you can change to something else:

```bash
configs.setProperty(BucketPriorityConfig.DELIMITER_CONFIG, "|");
```

### Discarding messages

Discarding any message that can't be sent to any of the buckets is also possible:

```bash
configs.setProperty(BucketPriorityConfig.FALLBACK_PARTITIONER_CONFIG,
   "code.buildon.aws.streaming.kafka.DiscardPartitioner");
```

## Using the assignor

To use the `BucketPriorityAssignor` in your consumer you need to register it in the configuration.

```bash
Properties configs = new Properties();

configs.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
   BucketPriorityAssignor.class.getName());

KafkaConsumer<K, V> consumer = new KafkaConsumer<>(configs);
```

To work properly, you need to specify in the configuration which topic will have its partitions grouped into buckets. This is important because, in Kafka, consumers can subscribe to multiple topics. This means that the same consumer can read messages from different topics, so the assignor needs to know which topic will have their partitions grouped into buckets.

```bash
configs.setProperty(BucketPriorityConfig.TOPIC_CONFIG, "orders");
```

You also have to specify in the configuration which buckets will be configured and what is the partition allocation for each one of them.
The partition allocation is specified in terms of percentage. Note that the usage of the symbol `%` is optional. Ideally, the partition allocation configuration needs to be the same used in the producer.


```bash
configs.setProperty(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
configs.setProperty(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
```

The assignor ensures that all partitions from the topic will be assigned to the buckets.
In case of the allocation result in some partitions being left behind because the distribution is not even, the remaining partitions will be assigned to the buckets using a round robin algorithm over the buckets sorted by allocation.

Finally you need to specify in the configuration which bucket the consumer will be associated.

```bash
configs.setProperty(BucketPriorityConfig.BUCKET_CONFIG, "Platinum");
```

### What about the other topics?

In Kafka, a consumer can subscribe to multiple topics, allowing the same consumer to read messages from partitions belonging to different topics. Because of this, the assignor ensures that only the topic specified in the configuration will have its partitions assigned to the consumers using the bucket priority logic. The other topics will have their partitions assigned to consumers using a fallback assignor.

Here is an example of configuring the fallback assignor to round-robin:

```bash
configs.setProperty(BucketPriorityConfig.FALLBACK_ASSIGNOR_CONFIG,
   "org.apache.kafka.clients.consumer.RoundRobinAssignor");
```

If you don't configure a fallback assignor explicitly, Kafka's default assignor will be used.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the MIT-0 License. See the [LICENSE](./LICENSE) file.
