package com.thenetcircle.service.data.hive.udf.kafka;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.deferedObj2Map;
import static com.thenetcircle.service.data.hive.udf.kafka.KafkaHelper.mapToProperties;
import static java.lang.String.format;


@Description(name = "kf_topics",
    value = "_FUNC_(map) - read all topics from kafka by settings in map parameter, returns an array of structs with properties of t(topic) and p(partition)")
@UDFType(deterministic = false, stateful = false, distinctLike = true)
public class UDFKafkaTopics extends GenericUDF {

    private transient MapObjectInspector settingInsp = null;
    private transient KafkaConsumer<String, String> consumer = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);

        if (!(args[0] instanceof MapObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + args[0]);
        }
        MapObjectInspector moi = (MapObjectInspector) args[0];
        if (!(moi.getMapKeyObjectInspector() instanceof StringObjectInspector && moi.getMapValueObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + moi);
        }
        settingInsp = moi;

        return KafkaHelper.KAFKA_TOPIC_PARTITION_INSP;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (ArrayUtils.isEmpty(args)) {
            return new Object[0];
        }

        StandardMapObjectInspector retInsp = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(settingInsp, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);

        Map<?, ?> cfgs = deferedObj2Map(args[0], settingInsp, retInsp);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(mapToProperties(cfgs))) {
            Map<String, List<PartitionInfo>> topicAndParts = consumer.listTopics();
            return topicAndParts.values().stream()
                .flatMap(List::stream)
                .map((PartitionInfo pi) -> new Object[]{pi.topic(), pi.partition()}).toArray();
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        Optional.ofNullable(consumer).ifPresent(KafkaConsumer::close);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("kf_topics(%s)", StringUtils.join(children, ", "));
    }
}
