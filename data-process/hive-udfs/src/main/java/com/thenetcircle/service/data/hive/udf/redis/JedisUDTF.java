package com.thenetcircle.service.data.hive.udf.redis;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import com.thenetcircle.service.data.hive.udf.commons.UDTFExt;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import redis.clients.jedis.Jedis;

import java.net.URI;
import java.net.URISyntaxException;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgsSize;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.getConverter;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

public abstract class JedisUDTF extends UDTFExt {
    protected transient Jedis jd;
    private String redisURIStr;
    private StructObjectInspector retObjInsp;

    protected ObjectInspector[] paramObjInsps = null;

    protected String jedisMethodName = null;

    protected UDFHelper.PrimitiveMethodBridge mb = null;

    protected abstract UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException;

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        checkArgsSize(funcName, argOIs, 1, 100);

        if (!(argOIs[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(1, "The redis uri should be string type.");
        }
        if (!(argOIs[0] instanceof ConstantObjectInspector)) {
            throw new UDFArgumentTypeException(1, "The redis uri should be a constant.");
        }

        ConstantObjectInspector uriInsp = (ConstantObjectInspector) argOIs[0];
        ObjectInspectorConverters.Converter converter = getConverter(uriInsp, javaStringObjectInspector);
        redisURIStr = (String) converter.convert(uriInsp.getWritableConstantValue());
//TODO
//        paramObjInsps = initParamInsps(ArrayUtils.subarray(argOIs, 1, argOIs.length));
        mb = initMethodBridge(ArrayUtils.subarray(argOIs, 1, argOIs.length));

        return (StructObjectInspector) mb.retObjInsp;
    }

    @Override
    public Object[] evaluate(Object[] _args, int start) throws HiveException {
        return new Object[0];
    }

    @Override
    public void close() throws HiveException {
        if (jd != null && jd.isConnected()) {
            jd.close();
            jd = null;
        }
    }

    @Override
    public void process(Object[] args) throws HiveException {
        setupJedis();
        Object[] results = ArrayUtils.add(evaluate(args, 2), args[0]);
        System.out.printf("args: %s\nresult: %s\n\n", ToStringBuilder.reflectionToString(args), ToStringBuilder.reflectionToString(results));
        forward(results);
    }

    protected void setupJedis() throws HiveException {
        if (jd == null || !jd.isConnected()) {
            jd = null;
            try {
                jd = new Jedis(new URI(redisURIStr));
            } catch (URISyntaxException e) {
                e.printStackTrace();
                throw new HiveException(e);
            }
        }
    }
}
