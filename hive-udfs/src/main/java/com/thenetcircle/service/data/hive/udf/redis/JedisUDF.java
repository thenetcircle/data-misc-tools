package com.thenetcircle.service.data.hive.udf.redis;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static com.thenetcircle.service.data.hive.udf.redis.JedisHelper.closeJedisPool;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.getConverter;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

public abstract class JedisUDF extends GenericUDF {
    private transient Jedis jd;
    private String redisURIStr;
    private static transient JedisPool jp = null;

    public void initializeRedis(ObjectInspector[] argOIs) throws UDFArgumentException {

        if (!(argOIs[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(1, "The redis uri should be string type.");
        }
        if (!(argOIs[0] instanceof ConstantObjectInspector)) {
            throw new UDFArgumentTypeException(1, "The redis uri should be a constant.");
        }

        ConstantObjectInspector uriInsp = (ConstantObjectInspector) argOIs[0];
        Converter converter = getConverter(uriInsp, javaStringObjectInspector);
        redisURIStr = (String) converter.convert(uriInsp.getWritableConstantValue());

        if (jp == null) {
            jp = new JedisPool();
        }
    }


    @Override
    public void close() throws IOException {
        closeJedisPool(jp);
    }
}
