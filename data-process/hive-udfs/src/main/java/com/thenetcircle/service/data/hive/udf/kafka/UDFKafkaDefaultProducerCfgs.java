package com.thenetcircle.service.data.hive.udf.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import static com.thenetcircle.service.data.hive.udf.kafka.KafkaHelper.KAFKA_CFG_INSP;
import static java.lang.String.format;

@Description(name = "kf_producer_cfgs",
    value = "_FUNC_() - return the map containing default settings for kafka producer")
@UDFType(deterministic = true, stateful = false, distinctLike = false)
public class UDFKafkaDefaultProducerCfgs extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 0, 0);
        return KAFKA_CFG_INSP;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        return KafkaHelper.DEFAULT_PRODUCER_CONFIGS;
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("kf_producer_cfgs(%s)", StringUtils.join(children, ", "));
    }
}
