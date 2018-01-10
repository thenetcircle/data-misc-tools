package com.thenetcircle.service.data.hive.udf.redis;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import redis.clients.jedis.JedisCommands;

import java.lang.reflect.Method;

@Description(name = "jd_set5", value = "set(Any context, String redisUri, String arg0, String arg1, String arg2, String arg3, long arg4) -> String")
public class UDTFJedisSet5 extends JedisUDTF {

    @Override
    protected UDFHelper.PrimitiveMethodBridge initMethodBridge(ObjectInspector[] argOIs) throws UDFArgumentException {
        Method setMd = null;
        try {
            setMd = JedisCommands.class.getDeclaredMethod("set", String.class, String.class, String.class, String.class, long.class);
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
        String nxxx = (String) mb.objInspAndConverters.get(2).getRight().convert(_args[2 + start]);
        String expx = (String) mb.objInspAndConverters.get(3).getRight().convert(_args[3 + start]);
        Long time = (Long) mb.objInspAndConverters.get(4).getRight().convert(_args[4 + start]);

        return new Object[]{jd.set(key, value, nxxx, expx, time)};
    }
}
