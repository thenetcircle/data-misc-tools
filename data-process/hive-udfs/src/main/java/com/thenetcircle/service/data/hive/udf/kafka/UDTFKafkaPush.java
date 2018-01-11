package com.thenetcircle.service.data.hive.udf.kafka;

import com.thenetcircle.service.data.hive.udf.commons.UDTFExt;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.kafka.KafkaHelper.mapToProperties;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;


@Description(name = "t_kf_push",
    value = "t_kf_push(Any context, Map<String, String> map, String topic, String key, String value) - pushWithTransaction records to kafka topics, it returns an array records of structs with properties of t(topic), ct(creation time), k(key) and v(value)")
public class UDTFKafkaPush extends UDTFExt {
    private static transient final Logger log = LoggerFactory.getLogger(UDTFKafkaPush.class);
    public static final String FUNC_NAME = "t_kf_push";

    private transient MapObjectInspector settingInsp = null;
    private transient KafkaProducer<String, String> producer = null;
    private transient StringObjectInspector topicInsp;
    private transient StandardMapObjectInspector retInsp;
    private transient StringObjectInspector recordKeyInsp = null;
    private transient StringObjectInspector recordValueInsp = null;
    private long timeoutForResult = 1000;

    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[4];

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(FUNC_NAME, args, 4, 4);

        if (!(args[0] instanceof MapObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + args[0]);
        }

        MapObjectInspector moi = (MapObjectInspector) args[0];
        if (!((moi.getMapKeyObjectInspector() instanceof StringObjectInspector)
            && (moi.getMapValueObjectInspector() instanceof StringObjectInspector))) {
            throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + args[0]);
        }
        settingInsp = moi;
        retInsp = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(settingInsp, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);

        checkArgPrimitive(FUNC_NAME, args, 1);
        checkArgGroups(FUNC_NAME, args, 1, inputTypes, STRING_GROUP);
        if (!(args[1] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "topic parameter must be string:\n\t" + args[1]);
        }
        this.topicInsp = StringObjectInspector.class.cast(args[1]);

        checkArgPrimitive(FUNC_NAME, args, 2);
        checkArgGroups(FUNC_NAME, args, 2, inputTypes, STRING_GROUP);
        if (!(args[2] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "key parameter must be string:\n\t" + args[2]);
        }
        this.recordKeyInsp = StringObjectInspector.class.cast(args[2]);

        checkArgPrimitive(FUNC_NAME, args, 3);
        checkArgGroups(FUNC_NAME, args, 3, inputTypes, STRING_GROUP);
        if (!(args[3] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "value parameter must be string:\n\t" + args[3]);
        }
        this.recordValueInsp = StringObjectInspector.class.cast(args[3]);

        return KafkaHelper.KAFKA_RECORD_INSP;
    }

    @Override
    public Object[] evaluate(Object[] args, int start) throws HiveException {
        if (producer == null) {
            Map<?, ?> cfgs = obj2Map(args[start + 0], settingInsp, retInsp);
            timeoutForResult = Optional.ofNullable(cfgs.get(REQUEST_TIMEOUT_MS_CONFIG))
                .map(String::valueOf)
                .map(str -> NumberUtils.toLong(str, timeoutForResult)).get();
            log.info("timeoutForResult:\t" + timeoutForResult);
            producer = new KafkaProducer<String, String>(mapToProperties(cfgs));
        }

        String topic = topicInsp.getPrimitiveJavaObject(args[start + 1]);

        String key = this.recordKeyInsp.getPrimitiveJavaObject(args[start + 2]);

        String value = this.recordValueInsp.getPrimitiveJavaObject(args[start + 3]);

        Future<RecordMetadata> recordMetadataFuture = KafkaHelper.pushWithOutTransaction(producer, topic, key, value);

        try {
            RecordMetadata rmd = recordMetadataFuture.get(timeoutForResult, TimeUnit.MILLISECONDS);
            return new Object[]{topic, rmd.timestamp(), key, value};
        } catch (Exception e) {
            e.printStackTrace();
            return new Object[]{topic, System.currentTimeMillis(), key, e.getMessage()};
        }
    }

    @Override
    public void close() throws HiveException {
        Optional.ofNullable(producer).ifPresent(KafkaProducer::close);
    }

    public String getDisplayString(String[] children) {
        return format("t_kf_push(%s)", StringUtils.join(children, ", "));
    }
}
