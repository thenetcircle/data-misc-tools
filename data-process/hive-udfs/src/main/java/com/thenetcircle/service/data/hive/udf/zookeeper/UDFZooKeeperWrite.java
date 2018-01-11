package com.thenetcircle.service.data.hive.udf.zookeeper;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper.DUMMY_WATCHER;
import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper._writeZKTree;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(
    name = "zk_write",
    value = "_FUNC_(String zkAddress, int timeout, Map pathAndValues) recursively write values paired with zookeeper paths")
@UDFType(deterministic = false, stateful = false, distinctLike = true, impliesOrder = true)
public class UDFZooKeeperWrite extends GenericUDF {
    private transient StringObjectInspector zkAddrInsp;
    private transient MapObjectInspector pathAndValueInsp;
    private int timeout = 3000;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

    private static transient final Logger log = LoggerFactory.getLogger(UDFZooKeeperWrite.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[args.length];

        checkArgsSize(args, 3, 3);
        checkArgPrimitive(args, 0);
        checkArgGroups(args, 0, inputTypes, STRING_GROUP);
        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "zkAddress parameter must be string:\n\t" + args[0]);
        }
        this.zkAddrInsp = StringObjectInspector.class.cast(args[0]);

        checkArgPrimitive(args, 1);
        checkArgGroups(args, 1, inputTypes, NUMERIC_GROUP);
        timeout = getConstantIntValue(args, 1);

        ObjectInspector _pathAndValueInsp = args[2];
        if (!(_pathAndValueInsp instanceof MapObjectInspector)) {
            throw new UDFArgumentTypeException(0, "pathAndValues parameter must be map:\n\t" + _pathAndValueInsp);
        }
        pathAndValueInsp = (MapObjectInspector) _pathAndValueInsp;

        return ZooKeeperHelper.RESULT_TYPE;
    }

    private transient ZooKeeper zk = null;

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        DeferredObject arg0 = args[0];
        if (arg0 == null) {
            throw new UDFArgumentException("zkAddr is null");
        }
        String zkAddr = zkAddrInsp.getPrimitiveJavaObject(arg0.get());
        if (StringUtils.isBlank(zkAddr)) {
            throw new UDFArgumentException("zkAddr is blank");
        }

        try {
            if (zk == null) {
                zk = new ZooKeeper(zkAddr, timeout, DUMMY_WATCHER);
                log.info("initialize ZooKeeper");
            }

            DeferredObject arg2 = args[2];
            Map<?, ?> raw = pathAndValueInsp.getMap(arg2.get());
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
    public void close() throws IOException {
        super.close();
        log.info("closeJedisPool ZooKeeper");
        ZooKeeperHelper.close(zk);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("zk_write(%s)", StringUtils.join(children, ", "));
    }
}
