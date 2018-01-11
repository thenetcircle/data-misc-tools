import com.thenetcircle.service.data.hive.udf.kafka.KafkaHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Kafka010Tests {

    static Logger log = LogManager.getLogger(Kafka010Tests.class);

    private static Properties getSettings() {
        Properties props = new Properties();
//        props.put("bootstrap.servers", "10.20.2.103:9092,10.20.2.102:9092,10.20.2.101:9092");
        props.put("bootstrap.servers", "troy:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    @Test
    public void test() {
        try (KafkaConsumer kc = new KafkaConsumer(getSettings())) {
            Map<String, List<PartitionInfo>> topicsAndPartitions = kc.listTopics();
            log.info(topicsAndPartitions.keySet());

            Date start = DateUtils.parseDate("2017-09-10", "yyyy-MM-dd");
            Date end = DateUtils.parseDate("2017-10-11", "yyyy-MM-dd");

            List<PartitionInfo> gaysParts = topicsAndPartitions.get("event-dating-user");
            log.info(StringUtils.join(gaysParts, "\n"));

            Map<TopicPartition, Long> topicPartitionAndLongs = gaysParts.stream().collect(Collectors.toMap(
                (PartitionInfo pi) -> new TopicPartition(pi.topic(), pi.partition()),
                (PartitionInfo pi) -> start.getTime()));

            if (false) {//doesn't work for message format elder than 0.9.00
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = kc.offsetsForTimes(topicPartitionAndLongs);
                log.info(StringUtils.join(offsetsForTimes.entrySet(), "\n"));
            }

//            SimpleConsumer sc = new SimpleConsumer();
        } catch (ParseException e) {
            log.error("failed to parse time", e);
        }
    }

    @Test
    public void testDefaultCfgs() throws IllegalAccessException {
        ConfigDef defaultCfgs = (ConfigDef) FieldUtils.readStaticField(ConsumerConfig.class, "CONFIG", true);
//        log.info(defaultCfgs);
//        log.info(ToStringBuilder.reflectionToString(defaultCfgs));
        log.info(StringUtils.join(
            defaultCfgs.configKeys().values().stream()
                .map(ck -> format("group=%s\timportance=%s\ttype\t=\t%s\t%s\t=\t%s\t", ck.group, ck.importance, ck.type, ck.name, String.valueOf(ck.defaultValue)))
                .collect(Collectors.toList()), "\n"));
    }

    @Test
    public void testDefaultConsumerCfgs() {
        log.info(KafkaHelper.DEFAULT_CONSUMER_CONFIGS);
    }

    @Test
    public void testDefaultProducerCfgs() {
        log.info(KafkaHelper.DEFAULT_PRODUCER_CONFIGS);
    }

    @Test
    public void testPoll() throws ParseException {
        Properties cfgs = new Properties();
        cfgs.putAll(KafkaHelper.DEFAULT_CONSUMER_CONFIGS);
        cfgs.put("bootstrap.servers", "troy:9092");
//        log.info(cfgs);

        try (KafkaConsumer<String, String> kc = new KafkaConsumer<String, String>(cfgs)) {

            List<ConsumerRecord<String, String>> reList = KafkaHelper.poll(kc,
                1000,
                DateUtils.parseDate("2017-12-26", "yyyy-MM-dd").getTime(),
                DateUtils.parseDate("2017-12-31", "yyyy-MM-dd").getTime(),
                "hive-udf0");

            reList.forEach(log::info);
        }
    }

    public static LinkedHashMap<String, String> mockRecords() {
        LinkedHashMap<String, String> records = new LinkedHashMap<>();
        StackTraceElement[] stes = Thread.currentThread().getStackTrace();
        String timeStr = DateFormatUtils.format(System.currentTimeMillis(), "yy-MM-dd hh:mm:ss");
        for (int i = 0, j = stes.length; i < j; i++) {
            records.put(String.format("%s-%d", timeStr, i), stes[i].toString());
        }
        return records;
    }

    @Test
    public void testPushWithOutTransaction() throws ParseException {
        Properties cfgs = new Properties();
        cfgs.putAll(KafkaHelper.DEFAULT_PRODUCER_CONFIGS);
//        cfgs.put(ProducerConfig.ACKS_CONFIG, "all");
//        cfgs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
//        cfgs.put(ProducerConfig.RETRIES_CONFIG, "1");
//        cfgs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        cfgs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "hive-transactional-id-" + System.currentTimeMillis());
        cfgs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "troy:9092,pc135:9092,thin:9092");

        cfgs.entrySet().forEach(log::info);

        LinkedHashMap<String, String> records = mockRecords();

//        records.entrySet().forEach(log::info);

        try (KafkaProducer<String, String> kp = new KafkaProducer<String, String>(cfgs)) {
            KafkaHelper.pushWithOutTransaction(kp, "hive-udf4", records);
        }
    }

    @Test
    public void testPushWithTransaction() throws ParseException {
        Properties cfgs = new Properties();
        cfgs.putAll(KafkaHelper.DEFAULT_PRODUCER_CONFIGS);
        cfgs.put(ProducerConfig.ACKS_CONFIG, "all");
        cfgs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        cfgs.put(ProducerConfig.RETRIES_CONFIG, "1");
        cfgs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        cfgs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "hive-transactional-id-" + System.currentTimeMillis());
        cfgs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "troy:9092,pc135:9092,thin:9092");

        cfgs.entrySet().forEach(log::info);

        LinkedHashMap<String, String> records = mockRecords();

        records.entrySet().forEach(log::info);

        try (KafkaProducer<String, String> kp = new KafkaProducer<String, String>(cfgs)) {
            KafkaHelper.pushWithTransaction(kp, "hive-udf0", records);
        }
    }
}
