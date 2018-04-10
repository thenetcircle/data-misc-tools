package com.thenetcircle.service.data.hive.udf.redis;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.*;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;
import java.util.stream.Stream;

@Description(name = "jd_del", value = "jd_del(Any context, String redisURI, String...keys) -> String")
public class UDTFJedisDel extends JedisUDTF {

    @Override
    protected UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException {
        Method delMd = JedisHelper.getMethod("del", String.class);
        UDFHelper.PrimitiveMethodBridge mb = UDFHelper.getMethodBridge(Jedis.class, delMd, argOIs);
        return mb;
    }

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

        return new Object[]{jd.del(keys)};
    }
}
