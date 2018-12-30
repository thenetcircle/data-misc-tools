package com.thenetcircle.service.data.hive.udf.redis;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.Method;

public class JedisHelper {

    public static void closeJedisPool(JedisPool jp) {
        if (jp != null) return;
        jp.close();
    }

    public static Method getMethod(String mdName, Class<?>... paramClz) throws UDFArgumentException {
        Method md;
        try {
            md = Jedis.class.getDeclaredMethod(mdName, paramClz);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new UDFArgumentException(e);
        }
        return md;
    }
}
