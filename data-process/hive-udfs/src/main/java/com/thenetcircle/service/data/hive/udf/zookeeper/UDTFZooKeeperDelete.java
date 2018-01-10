package com.thenetcircle.service.data.hive.udf.zookeeper;

import com.thenetcircle.service.data.hive.udf.commons.UDTFExt;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper.*;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(
    name = "t_zk_delete",
    value = "_FUNC_(zkAddress, timeout, pathToDeletes) recursively delete zookeeper paths")
public class UDTFZooKeeperDelete extends UDTFExt {
    public static final String FUNC_NAME = "t_zk_delete";
    private transient StringObjectInspector zkAddrInsp;
    private transient StringObjectInspector pathsInsp;
    private int timeout = 3000;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

    private static transient final Logger log = LoggerFactory.getLogger(UDTFZooKeeperDelete.class);

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[args.length];

        checkArgsSize(FUNC_NAME, args, 3, Integer.MAX_VALUE);
        checkArgPrimitive(FUNC_NAME, args, 0);
        checkArgGroups(FUNC_NAME, args, 0, inputTypes, STRING_GROUP);

        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "zkAddress parameter must be string:\n\t" + args[0]);
        }
        this.zkAddrInsp = StringObjectInspector.class.cast(args[0]);

        checkArgPrimitive(FUNC_NAME, args, 1);
        checkArgGroups(FUNC_NAME, args, 1, inputTypes, NUMERIC_GROUP);
        timeout = getConstantIntValue(FUNC_NAME, args, 1);

        for (int i = 2, j = args.length; i < j; i++) {
            checkArgPrimitive(FUNC_NAME, args, i);
            checkArgGroups(FUNC_NAME, args, i, inputTypes, STRING_GROUP);
            if (!(args[i] instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(0, "path parameter must be string:\n\t" + args[i]);
            }
        }
        pathsInsp = (StringObjectInspector) args[2];

        return ZooKeeperHelper.PATH_VALUE_STRUCT;
    }

    private transient ZooKeeper zk = null;

    @Override
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
            }

            List<String> _pathList = new ArrayList<>(args.length - 2);
            for (int i = start, j = args.length; i < j; i++) {
//            for (Object pathObj : ArrayUtils.subarray(args, start + 2, args.length)) {
                Object pathObj = args[i];
                String javaObj = pathsInsp.getPrimitiveJavaObject(pathObj);
                if (StringUtils.isNotBlank(javaObj))
                    _pathList.add(javaObj);
            }

            List<String> pathList = mergePaths(_pathList.toArray(new String[0]));

            LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
            resultMap.putAll(deleteZKTree(zk, pathList.get(0)));
            for (int i = 1, j = pathList.size(); i < j; i++) {
                String path = pathList.get(i);
                if (resultMap.containsKey(path)) continue;
                resultMap.putAll(deleteZKTree(zk, path));
            }

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
    public void process(Object[] args) throws HiveException {
        Object[] evaluated = evaluate(args, 1);

        for (Object pathAndValue : evaluated) {
            Object[] row = (Object[]) pathAndValue;
            forward(ArrayUtils.add(row, args[0]));
        }
    }

    @Override
    public void close() {
        ZooKeeperHelper.close(zk);
    }

    public String getDisplayString(String[] children) {
        return format("zk_delete(%s)", StringUtils.join(children, ", "));
    }
}
