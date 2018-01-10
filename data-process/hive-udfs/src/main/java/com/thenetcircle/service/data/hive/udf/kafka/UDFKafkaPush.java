package com.thenetcircle.service.data.hive.udf.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Optional;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;


@Description(name = "kf_push",
    value = "_FUNC_(map, topic, array<struct<k,v>>) - pushWithTransaction records to kafka topics, it returns an array records of structs with properties of k(key) and v(value)")
@UDFType(deterministic = false, stateful = false, distinctLike = true)
public class UDFKafkaPush extends GenericUDF {

    private transient MapObjectInspector settingInsp = null;
    private transient KafkaProducer<String, String> producer = null;
    private transient StringObjectInspector topicInsp;

    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[3];

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 3, 3);

        if (!(args[0] instanceof MapObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + args[0]);
        }
        MapObjectInspector moi = (MapObjectInspector) args[0];
        if (!((moi.getMapKeyObjectInspector() instanceof StringObjectInspector)
            && (moi.getMapValueObjectInspector() instanceof StringObjectInspector))) {
            throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + args[0]);
        }
        settingInsp = moi;

        checkArgPrimitive(args, 1);
        checkArgGroups(args, 1, inputTypes, STRING_GROUP);
        if (!(args[1] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "topic parameter must be string:\n\t" + args[0]);
        }
        this.topicInsp = StringObjectInspector.class.cast(args[0]);

        return KafkaHelper.KAFKA_REC_INSP;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        //TODO
        return null;
    }

    @Override
    public void close() throws IOException {
        super.close();
        Optional.ofNullable(producer).ifPresent(KafkaProducer::close);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("kf_push(%s)", StringUtils.join(children, ", "));
    }
}
