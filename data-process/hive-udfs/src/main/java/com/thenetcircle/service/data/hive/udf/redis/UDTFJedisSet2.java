package com.thenetcircle.service.data.hive.udf.redis;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import redis.clients.jedis.JedisCommands;

import java.lang.reflect.Method;

@Description(name = "jd_set2", value = "set(Any context, String redisUri, String arg0, String arg1) -> String")
public class UDTFJedisSet2 extends JedisUDTF {

    @Override
    protected UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException {
        Method setMd = null;
        try {
            setMd = JedisCommands.class.getDeclaredMethod("set", String.class, String.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new UDFArgumentException(e);
        }

        UDFHelper.PrimitiveMethodBridge mb = UDFHelper.getMethodBridge(JedisCommands.class, setMd, argOIs);

        return mb;
    }

    @Override
    public Object[] evaluate(Object[] _args, int start) throws HiveException {
        String key = (String) mb.objInspAndConverters.get(0).getRight().convert(_args[0 + start]);
        String value = (String) mb.objInspAndConverters.get(1).getRight().convert(_args[1 + start]);

        return new Object[]{jd.set(key, value)};
    }
}
