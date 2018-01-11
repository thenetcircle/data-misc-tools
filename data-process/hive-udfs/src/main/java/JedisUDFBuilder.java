import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class JedisUDFBuilder {
    static Logger log = LogManager.getLogger(JedisUDFBuilder.class);

    @Test
    public void listMethods() {
        Class<Jedis> clz = Jedis.class;
        log.info(StringUtils.join(clz.getDeclaredMethods(), "\n"));
    }

    @Test
    public void listParameters() {
        Class<Jedis> clz = Jedis.class;
        Method[] mds = clz.getDeclaredMethods();
        IntStream.range(0, mds.length)
            .forEach(i -> {
                Method m = mds[i];
                System.out.println(i + "\t" + m.getName() + "\t-> " + m.getReturnType());

                Stream.of(m.getParameters()).forEach(p -> {
                    System.out.println("\t" + p);
                });
                System.out.println();
            });
    }

    @Test
    public void tryParameterOIs() {
        Class<JedisCommands> clz = JedisCommands.class;
        Method[] mds = clz.getDeclaredMethods();

        Arrays.sort(mds, Comparator.comparing((Method m) -> m.getName()));

        IntStream.range(0, mds.length)
            .forEach(i -> {

                Method m = mds[i];
                System.out.println(i + "\t" + m.getName() + "\t-> " + m.getReturnType());

                Stream.of(m.getParameters()).forEach(p -> {
                    System.out.println("\t" + p);
                });
                System.out.println();
                ObjectInspector retObjInsp = UDFHelper.retType2ObjInsp(m);
                if (retObjInsp == null) {
                    System.out.println();
                    return;
                }
                System.out.println(retObjInsp.getClass().getSimpleName());

                System.out.println();
                System.out.println();
            });
    }

    @Test
    public void tryDescription() {
        Class<JedisCommands> clz = JedisCommands.class;
        Method[] mds = clz.getDeclaredMethods();

        Arrays.sort(mds, Comparator.comparing((Method m) -> m.getName()));

        IntStream.range(0, mds.length)
            .forEach(i -> {
                Method m = mds[i];
                System.out.printf("%s(Any context, String redisUri, %s) -> %s\n", m.getName(),
                    StringUtils.join(Stream.of(m.getParameters()).map(p -> p.getType().getSimpleName() + " " + p.getName()).toArray(), ", "),
                    m.getReturnType().getSimpleName());
            });
    }
}
