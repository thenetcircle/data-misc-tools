package com.thenetcircle.service.data.hive.udf.redis;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;

@Description(name = "jd_mget", value = "jd_mget(Any Context, String redisUrl, String...keys) -> String[]")
public class UDTFJedisMultiGet extends JedisUDTF {

    @Override
    protected UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException {
        Method mgetMd = JedisHelper.getMethod("mget", String[].class);
        UDFHelper.PrimitiveMethodBridge mb = UDFHelper.getMethodBridge(Jedis.class, mgetMd, argOIs);
        return mb;
    }

    //TODO org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
    @Override
    public Object[] evaluate(Object[] _args, int start) throws HiveException {
        Pair<ObjectInspector, Converter> inspAndConverter = mb.objInspAndConverters.get(0);
        Converter converter = inspAndConverter.getRight();
        String[] keys = Stream.of(_args)
            .skip(start)
            .map(arg -> converter.convert(arg))
            .filter(arg -> arg != null)
            .map(arg -> String.valueOf(arg))
            .toArray(String[]::new);
        System.out.println("_args = [" + ToStringBuilder.reflectionToString(_args) + "], start = [" + start + "]");
        return jd.mget(keys).toArray();
    }
}
