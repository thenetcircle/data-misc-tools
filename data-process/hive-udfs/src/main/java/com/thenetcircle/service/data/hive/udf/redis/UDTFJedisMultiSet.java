package com.thenetcircle.service.data.hive.udf.redis;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;

import java.lang.reflect.Method;
import java.util.stream.Stream;

@Description(name = "jd_mset", value = "jd_mset(Any Context, String redisUrl, String...keys) -> String[]")
public class UDTFJedisMultiSet extends JedisUDTF {

    @Override
    protected UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException {
        Method msetMd = JedisHelper.getMethod("mset", String[].class);
        UDFHelper.PrimitiveMethodBridge mb = UDFHelper.getMethodBridge(Jedis.class, msetMd, argOIs);
        return mb;
    }

    @Override
    public Object[] evaluate(Object[] _args, int start) throws HiveException {

        Pair<ObjectInspector, ObjectInspectorConverters.Converter> inspAndConverter = mb.objInspAndConverters.get(0);
        ObjectInspectorConverters.Converter converter = inspAndConverter.getRight();
        String[] keys = Stream.of(_args)
            .skip(start)
            .map(arg -> converter.convert(arg))
            .filter(arg -> arg != null)
            .map(arg -> String.valueOf(arg))
            .toArray(String[]::new);

        return new Object[]{jd.mset(keys)};
    }
}
