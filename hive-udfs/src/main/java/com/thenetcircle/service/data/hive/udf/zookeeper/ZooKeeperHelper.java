package com.thenetcircle.service.data.hive.udf.zookeeper;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;

public class ZooKeeperHelper {
    public static final StandardStructObjectInspector PATH_VALUE_STRUCT = ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList("p", "v"),
            Arrays.asList(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector));

    public static final StandardListObjectInspector RESULT_TYPE = ObjectInspectorFactory.getStandardListObjectInspector(PATH_VALUE_STRUCT);

    public static Map<String, String> _writeZKTree(ZooKeeper zk, Map<String, String> pathAndValues) throws KeeperException, InterruptedException {
        Map<String, String> pathAndOldValues = new HashMap<>();
        if (MapUtils.isEmpty(pathAndValues)) return pathAndOldValues;

        List<String> paths = new ArrayList<>(pathAndValues.keySet());
        Collections.sort(paths);

        String lastPath = paths.get(0);
        ensurePath(zk, lastPath, null);
        String oldValue1 = createOrSet(zk, lastPath, pathAndValues.get(lastPath));
        pathAndOldValues.put(lastPath, oldValue1);

        for (int i = 1, len = paths.size(); i < len; i++) {
            String path = paths.get(i);
            int lastSep = StringUtils.lastIndexOf(path, '/');
            String _path = StringUtils.left(path, lastSep + 1);
            String prefix = StringUtils.getCommonPrefix(lastPath, _path);
            String between = StringUtils.substring(_path, prefix.length());
            ensurePath(zk, between, prefix);
            String oldValue = createOrSet(zk, path, pathAndValues.get(path));
            if (oldValue != null) {
                pathAndOldValues.put(path, oldValue);
            }
            lastPath = path;
        }
        return pathAndOldValues;
    }

    public static Map<String, String> _readZKTree(ZooKeeper zk, String startPath) throws KeeperException, InterruptedException {
        startPath = StringUtils.defaultIfBlank(startPath, "/");

        Stack<String> stack = new Stack<>();
        LinkedHashMap<String, String> pathAndDatas = new LinkedHashMap<>();
        String path = startPath;
        stack.push(path);

        if (zk.exists(path, false) == null) {
            System.err.printf("%s\t=>\t%s\n", path, "not existent");
            return pathAndDatas;
        }

        Stat stat = new Stat();
        do {
            String _path = stack.pop();

            byte[] data = zk.getData(_path, false, stat);
            String value = data == null ? null : new String(data);
            pathAndDatas.put(_path, value);

            if (stat.getNumChildren() != 0) {
                List<String> children = zk.getChildren(_path, false);
                children.stream().map((String child) -> _path + (_path.endsWith("/") ? "" : "/") + child).forEach(stack::push);
            }
        } while (!stack.empty());

        return pathAndDatas;
    }

//    public static Map<String, String> _writeZKTree(ZooKeeper zk, String startPath, Map<String, Object> pathAndValues) throws KeeperException, InterruptedException {
//        startPath = StringUtils.defaultIfBlank(startPath, "/");
//        for (Map.Entry<String, Object> pathAndValue : pathAndValues.entrySet()) {
//            zk.create()
//        }
//    }

    public static String createOrSet(ZooKeeper zk, String path, String value) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, null);
        byte[] bytes = value.getBytes();
        System.out.printf("writing value: %s\tto path: %s\n", value, path);
        if (stat == null) {
            zk.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return null;
        }
        byte[] oldData = zk.getData(path, null, stat);
        stat = zk.setData(path, bytes, -1);
        return new String(oldData);
    }

    public static String getAndSet(ZooKeeper zk, String path, String value) throws KeeperException, InterruptedException {
        byte[] oldData = zk.getData(path, null, null);
        zk.setData(path, value.getBytes(), -1);
        return new String(oldData);
    }

    public static void ensurePath(ZooKeeper zk, String path, String startPath) throws KeeperException, InterruptedException {
        if (StringUtils.isBlank(path)) return;

        startPath = StringUtils.defaultIfBlank(startPath, "");
        for (int sepIdx = StringUtils.indexOf(path, '/', 1);
             sepIdx != -1;
             sepIdx = StringUtils.indexOf(path, '/', sepIdx + 1)) {

            String _path = startPath + StringUtils.left(path, sepIdx);
            Stat stat = zk.exists(_path, false);
            if (stat == null) {
                zk.create(_path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    public static Map<String, String> deleteZKTree(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        if (zk == null || StringUtils.isBlank(path) || "/".equals(path))
            return new HashMap<>();

        Map<String, String> pathAndValues = _readZKTree(zk, path);

        List<String> paths = new ArrayList<>(pathAndValues.keySet());
        Collections.sort(paths, Comparator.<String>naturalOrder().reversed());
        for (String _path : paths) {
            zk.delete(_path, -1);
        }

        return pathAndValues;
    }

    public static void close(ZooKeeper zk) {
        if (zk == null) return;
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static final Watcher DUMMY_WATCHER = new ZooKeeperHiveHelper.DummyWatcher();

    public static List<String> mergePaths(String[] paths) {
        if (ArrayUtils.isEmpty(paths)) return Collections.emptyList();

        if (paths.length == 1) return Arrays.asList(paths);

        Arrays.sort(paths);

        List<String> merged = new ArrayList<>();
        String lastPath = paths[0];
        merged.add(lastPath);

        for (int i = 0, j = paths.length; i < j; i++) {
            String path = paths[i];
            if (path.startsWith(lastPath)) continue;
            merged.add(path);
            lastPath = path;
        }

        return merged;
    }

    static LinkedHashMap<String, String> readZKPaths(List<String> _pathList, ZooKeeper zk) throws KeeperException, InterruptedException {
        List<String> pathList = mergePaths(_pathList.toArray(new String[0]));

        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        resultMap.putAll(_readZKTree(zk, pathList.get(0)));
        for (int i = 1, j = pathList.size(); i < j; i++) {
            String path = pathList.get(i);
            if (resultMap.containsKey(path)) continue;
            resultMap.putAll(_readZKTree(zk, path));
        }
        return resultMap;
    }
}
