package com.thenetcircle.service.data.hive.udf.zookeeper;

import com.thenetcircle.service.data.hive.udf.commons.UDTFExt;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper.DUMMY_WATCHER;
import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper._writeZKTree;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(
    name = "t_zk_write",
    value = "_FUNC_(Any context, String zkAddress, int timeout, Map<String, String> pathAndValues) recursively write values paired with zookeeper paths")
public class UDTFZooKeeperWrite extends UDTFExt {
    private static transient final Logger log = LoggerFactory.getLogger(UDTFZooKeeperWrite.class);

    public static final String FUNC_NAME = "t_zk_write";

    private transient StringObjectInspector zkAddrInsp;
    private transient MapObjectInspector pathAndValueInsp;
    private int timeout = 3000;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[args.length];

        checkArgsSize(FUNC_NAME, args, 3, 3);
        checkArgPrimitive(FUNC_NAME, args, 0);
        checkArgGroups(FUNC_NAME, args, 0, inputTypes, STRING_GROUP);
        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "zkAddress parameter must be string:\n\t" + args[0]);
        }
        this.zkAddrInsp = StringObjectInspector.class.cast(args[0]);

        checkArgPrimitive(FUNC_NAME, args, 1);
        checkArgGroups(FUNC_NAME, args, 1, inputTypes, NUMERIC_GROUP);
        timeout = getConstantIntValue(FUNC_NAME, args, 1);

        ObjectInspector _pathAndValueInsp = args[2];
        if (!(_pathAndValueInsp instanceof MapObjectInspector)) {
            throw new UDFArgumentTypeException(0, "pathAndValues parameter must be map:\n\t" + _pathAndValueInsp);
        }
        pathAndValueInsp = (MapObjectInspector) _pathAndValueInsp;

        return ZooKeeperHelper.PATH_VALUE_STRUCT;
    }

    private transient ZooKeeper zk = null;

    public Object[] evaluate(Object[] args, int start) throws HiveException {
        Object arg0 = args[start + 0];
        if (arg0 == null) {
            throw new UDFArgumentException("zkAddr is null");
        }
        String zkAddr = zkAddrInsp.getPrimitiveJavaObject(arg0);
        if (StringUtils.isBlank(zkAddr)) {
            throw new UDFArgumentException("zkAddr is blank");
        }

        try {
            if (zk == null) {
                zk = new ZooKeeper(zkAddr, timeout, DUMMY_WATCHER);
                log.info("initialize ZooKeeper");
            }

            Object arg2 = args[start + 2];
            Map<?, ?> raw = pathAndValueInsp.getMap(arg2);
            if (MapUtils.isEmpty(raw)) {
                return new Object[0];
            }

            Map<String, String> pathAndValues = new HashMap<>();
            for (Map.Entry pair : raw.entrySet()) {
                Object key = pair.getKey(), value = pair.getValue();
                if (key == null || value == null) continue;
                String keyStr = key.toString(), valueStr = value.toString();
                if (StringUtils.isBlank(keyStr)) continue;
                pathAndValues.put(keyStr, valueStr);
            }

            Map<String, String> pathAndOldValues = _writeZKTree(zk, pathAndValues);

            return pathAndOldValues.entrySet().stream()
                .map(entry -> new Object[]{entry.getKey(), entry.getValue()})
                .toArray();

        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
            ZooKeeperHelper.close(zk);
            zk = null;
        }

        return null;
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Object[] evaluated = evaluate(args, 1);

        for (Object pathAndValue : evaluated) {
            Object[] row = (Object[]) pathAndValue;
            forward(ArrayUtils.add(row, args[0]));
        }
    }

    @Override
    public void close() {
        log.info("closeJedisPool ZooKeeper");
        ZooKeeperHelper.close(zk);
    }

    public String getDisplayString(String[] children) {
        return format("zk_write(%s)", StringUtils.join(children, ", "));
    }
}
