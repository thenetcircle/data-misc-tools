package com.thenetcircle.service.data.hive.udf.kafka;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class KafkaHelper {
    public static final MapObjectInspector KAFKA_CFG_INSP = ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector
    );

    public static final StandardStructObjectInspector KAFKA_RECORD_INSP = ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList("t", "ct", "k", "v"),
        Arrays.asList(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        )
    );

    public static final StandardListObjectInspector KAFKA_RECORD_LIST_INSP = ObjectInspectorFactory.getStandardListObjectInspector(
        KAFKA_RECORD_INSP
    );

    public static final StandardListObjectInspector KAFKA_TOPIC_PARTITION_INSP = ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList("t", "p"),
            Arrays.asList(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
            )
        )
    );

    static final Set<ConfigDef.Type> printableTypes = new HashSet<>(Arrays.asList(
        ConfigDef.Type.BOOLEAN,
        ConfigDef.Type.DOUBLE,
        ConfigDef.Type.INT,
        ConfigDef.Type.LONG,
        ConfigDef.Type.STRING));

    static final Map<String, Object> configDefsToMap(ConfigDef cfgs) {
        Map<String, Object> defaultCfgMap = cfgs.configKeys().values().stream()
            .filter(ck -> printableTypes.contains(ck.type))
            .filter(ck -> ck.defaultValue != null)
            .collect(Collectors.toMap(ck -> ck.name, ck -> Optional.ofNullable(ck.defaultValue).orElse("null")));
        return defaultCfgMap;
    }

    public static final Properties DEFAULT_CONSUMER_CONFIGS = new Properties();

    static {
        try {
            ConfigDef defaultCfgs = (ConfigDef) FieldUtils.readStaticField(ConsumerConfig.class, "CONFIG", true);
            DEFAULT_CONSUMER_CONFIGS.putAll(configDefsToMap(defaultCfgs));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        //        DEFAULT_CONSUMER_CONFIGS.put("bootstrap.servers", "localhost:9092");
        DEFAULT_CONSUMER_CONFIGS.put(GROUP_ID_CONFIG, "hive-udf");
        DEFAULT_CONSUMER_CONFIGS.put(ConsumerConfig.CLIENT_ID_CONFIG, "hive-udf");
        DEFAULT_CONSUMER_CONFIGS.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        DEFAULT_CONSUMER_CONFIGS.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        DEFAULT_CONSUMER_CONFIGS.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "11001");
        DEFAULT_CONSUMER_CONFIGS.put(SESSION_TIMEOUT_MS_CONFIG, "11000");
        DEFAULT_CONSUMER_CONFIGS.put(HEARTBEAT_INTERVAL_MS_CONFIG, "800");
        DEFAULT_CONSUMER_CONFIGS.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        DEFAULT_CONSUMER_CONFIGS.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    public static final Properties DEFAULT_PRODUCER_CONFIGS = new Properties();

    static {
        try {
            ConfigDef defaultCfgs = (ConfigDef) FieldUtils.readStaticField(ProducerConfig.class, "CONFIG", true);
            DEFAULT_PRODUCER_CONFIGS.putAll(configDefsToMap(defaultCfgs));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        DEFAULT_PRODUCER_CONFIGS.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        DEFAULT_PRODUCER_CONFIGS.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        DEFAULT_PRODUCER_CONFIGS.put(COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name);
    }

    public static List<PartitionInfo> getTopics(Properties settings) {
        if (MapUtils.isEmpty(settings)) return Collections.emptyList();

        try (KafkaConsumer kc = new KafkaConsumer(settings)) {
            Map<String, List<PartitionInfo>> topicsAndPartitions = kc.listTopics();
            return topicsAndPartitions.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    public static Properties mapToProperties(Map map) {
        if (MapUtils.isEmpty(map)) return null;
        Properties re = new Properties();
        re.putAll(map);
        return re;
    }

    public static Future<RecordMetadata> pushWithOutTransaction(KafkaProducer<String, String> kp, String topic, String key, String value) {
        return kp.send(new ProducerRecord<>(topic, key, value));
    }

    public static List<Future<RecordMetadata>> pushWithOutTransaction(KafkaProducer<String, String> kp, String topic, LinkedHashMap<String, String> records) {
        List<Future<RecordMetadata>> futureList = new ArrayList<>();
        for (Map.Entry<String, String> record : records.entrySet()) {
            System.out.printf("sending...%s\n", record);
            futureList.add(kp.send(new ProducerRecord<>(topic, record.getKey(), record.getValue())));
        }
        return futureList;
    }

    public static List<Future<RecordMetadata>> pushWithTransaction(KafkaProducer<String, String> kp, String topic, LinkedHashMap<String, String> records) {
        List<Future<RecordMetadata>> futureList = new ArrayList<>();
        try {
            kp.initTransactions();
            kp.beginTransaction();
            for (Map.Entry<String, String> record : records.entrySet()) {
                System.out.printf("sending...%s\n", record);
                futureList.add(kp.send(new ProducerRecord<>(topic, record.getKey(), record.getValue())));
            }
            kp.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to closeJedisPool the producer and exit.
            e.printStackTrace();
            kp.close();
        } catch (KafkaException e) {
            e.printStackTrace();
            // For all other exceptions, just abort the transaction and try again.
            kp.abortTransaction();
        }
        return futureList;
    }

    public static List<ConsumerRecord<String, String>> poll(KafkaConsumer<String, String> kc,
                                                            long timeout,
                                                            Long start,
                                                            Long end,
                                                            String... topics) {
        Map<String, List<PartitionInfo>> topicAndParts = kc.listTopics();

//        topicAndParts.entrySet().forEach(System.out::println);

        Collection<String> validTopics = CollectionUtils.intersection(Arrays.asList(topics), topicAndParts.keySet());

        if (CollectionUtils.isEmpty(validTopics)) {
            System.out.printf("topics %s do not exist\n", StringUtils.join(topics, ','));
            return Collections.emptyList();
        }

        Map<TopicPartition, Long> topicAndTimesStart = new HashMap<>();
        validTopics.stream().forEach(topic -> {
            List<PartitionInfo> parts = topicAndParts.get(topic);
            parts.forEach(part -> topicAndTimesStart.put(new TopicPartition(part.topic(), part.partition()), start));
        });
        Map<TopicPartition, OffsetAndTimestamp> topicPartAndOffsetNTimesStart = kc.offsetsForTimes(topicAndTimesStart);

        Map<TopicPartition, Long> topicAndTimesEnd = new HashMap<>();
        validTopics.stream().forEach(topic -> {
            List<PartitionInfo> parts = topicAndParts.get(topic);
            parts.forEach(part -> topicAndTimesEnd.put(new TopicPartition(part.topic(), part.partition()), end));
        });
        Map<TopicPartition, OffsetAndTimestamp> topicPartAndOffsetNTimesEnd = kc.offsetsForTimes(topicAndTimesEnd);

        Set<TopicPartition> topicPartitions = topicPartAndOffsetNTimesStart.keySet();
        kc.assign(topicPartitions);

        Map<TopicPartition, Long> topicPartAndOldests = kc.beginningOffsets(topicPartitions);
        Map<TopicPartition, Long> topicPartAndLatests = kc.endOffsets(topicPartitions);

        Object[][] params = validTopics.stream().map(topicAndParts::get)
            .flatMap(List::stream)
            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
            .map((TopicPartition tp) -> {
                Long fromInclusive = Optional.ofNullable(topicPartAndOffsetNTimesStart.get(tp))
                    .map(OffsetAndTimestamp::offset)
                    .orElse(topicPartAndOldests.get(tp));

                Long toInclusive = Optional.ofNullable(topicPartAndOffsetNTimesEnd.get(tp))
                    .map(OffsetAndTimestamp::offset)
                    .orElse(topicPartAndLatests.get(tp));

                System.out.printf("tp:%s\t offset from: %s\t at time: %s to: %s at time: %s\n", tp, fromInclusive, start, toInclusive, end);

                return new Object[]{tp, fromInclusive, toInclusive};
            }).toArray(Object[][]::new);

        long totalCount = Stream.of(params).mapToLong(param -> {
            if (param[1] == null || param[2] == null) return 0;
            else return ((Long) param[2]) - ((Long) param[1]);
        }).sum();

        List<ConsumerRecord<String, String>> reList = new ArrayList<ConsumerRecord<String, String>>((int) totalCount);

        for (Object[] param : params) {
            TopicPartition tp = (TopicPartition) param[0];
            Long _start = (Long) param[1];
            Long _end = (Long) param[2];
            if (_start == null || _end == null) continue;

            try {
                kc.seek(tp, _start);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            for (long i = _start, j = _end - 1; i < j; ) {
                ConsumerRecords<String, String> recs = kc.poll(timeout);
                reList.addAll(recs.records(tp));
                i += recs.count();
            }
        }

        return reList;
    }

    public static Object[] kfRecord2HiveStruct(ConsumerRecord<String, String> cr) {
        return new Object[]{cr.topic(), cr.timestamp(), cr.key(), cr.value()};
    }
}
