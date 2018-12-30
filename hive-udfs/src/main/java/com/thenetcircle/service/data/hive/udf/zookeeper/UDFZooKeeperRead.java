package com.thenetcircle.service.data.hive.udf.zookeeper;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper.DUMMY_WATCHER;
import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper.readZKPaths;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(
    name = "zk_read",
    value = "_FUNC_(String zkAddress, int timeout, String...pathToReads...) recursively read data from zookeeper paths")
@UDFType(deterministic = false, stateful = false, distinctLike = true)
public class UDFZooKeeperRead extends GenericUDF {
    private transient StringObjectInspector zkAddrInsp;
    private transient StringObjectInspector pathsInsp;
    private int timeout = 3000;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[args.length];

        checkArgsSize(args, 3, Integer.MAX_VALUE);
        checkArgPrimitive(args, 0);
        checkArgGroups(args, 0, inputTypes, STRING_GROUP);

        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "zkAddress parameter must be string:\n\t" + args[0]);
        }
        this.zkAddrInsp = StringObjectInspector.class.cast(args[0]);

        checkArgPrimitive(args, 1);
        checkArgGroups(args, 1, inputTypes, NUMERIC_GROUP);
        timeout = getConstantIntValue(args, 1);

        for (int i = 2, j = args.length; i < j; i++) {
            checkArgPrimitive(args, i);
            checkArgGroups(args, i, inputTypes, STRING_GROUP);
            if (!(args[i] instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(0, "path parameter must be string:\n\t" + args[i]);
            }
        }
        pathsInsp = (StringObjectInspector) args[2];

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
            }

            List<String> _pathList = new ArrayList<>(args.length - 2);
            for (DeferredObject pathObj : ArrayUtils.subarray(args, 2, args.length)) {
                String javaObj = pathsInsp.getPrimitiveJavaObject(pathObj.get());
                if (StringUtils.isNotBlank(javaObj))
                    _pathList.add(javaObj);
            }

            LinkedHashMap<String, String> resultMap = readZKPaths(_pathList, zk);

            return resultMap.entrySet().stream()
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
        ZooKeeperHelper.close(zk);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("zk_read(%s)", StringUtils.join(children, ", "));
    }
}
