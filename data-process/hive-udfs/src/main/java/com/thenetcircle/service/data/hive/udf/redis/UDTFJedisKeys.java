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

@Description(name = "jd_keys", value = "jd_keys(Any context, String redisURI, String pattern) -> String")
public class UDTFJedisKeys extends JedisUDTF {

    @Override
    protected UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException {
        Method keysMd = JedisHelper.getMethod("keys", String.class);
        UDFHelper.PrimitiveMethodBridge mb = UDFHelper.getMethodBridge(Jedis.class, keysMd, argOIs);

        return mb;
    }

    @Override
    public Object[] evaluate(Object[] _args, int start) throws HiveException {

        Pair<ObjectInspector, ObjectInspectorConverters.Converter> inspAndConverter = mb.objInspAndConverters.get(0);
        ObjectInspectorConverters.Converter converter = inspAndConverter.getRight();
        String key = (String) converter.convert(_args[0]);

        return new Object[]{jd.get(key)};
    }
}
