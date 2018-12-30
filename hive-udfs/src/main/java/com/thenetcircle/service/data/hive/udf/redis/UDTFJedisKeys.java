package com.thenetcircle.service.data.hive.udf.redis;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;

@Description(name = "jd_keys", value = "jd_keys(Any context, String redisURI, String pattern) -> String")
public class UDTFJedisKeys extends JedisUDTF {
    static final Logger log = LoggerFactory.getLogger(UDTFJedisKeys.class);

    @Override
    protected UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException {
        Method keysMd = JedisHelper.getMethod("keys", String.class);
        UDFHelper.PrimitiveMethodBridge mb = UDFHelper.getMethodBridge(Jedis.class, keysMd, argOIs);
        return mb;
    }

    @Override
    public Object[] evaluate(Object[] _args, int start) throws HiveException {
        Pair<ObjectInspector, Converter> inspAndConverter = mb.objInspAndConverters.get(0);
        Converter converter = inspAndConverter.getRight();
        String key = (String) converter.convert(_args[0 + start]);
        return jd.keys(key).toArray();
    }

    @Override
    public void process(Object[] args) throws HiveException {
        setupJedis();

        for (Object row : evaluate(args, 2)) {
            Object[] results = new Object[]{args[0], row};
            log.debug(format("args: %s\nresult: %s\n\n", ToStringBuilder.reflectionToString(args),
                ToStringBuilder.reflectionToString(results)));
            forward(results);
        }
    }
}
