import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.*;

import static com.thenetcircle.service.data.hive.udf.zookeeper.ZooKeeperHelper.*;

public class ZookeeperTests {

    interface ExConsumer<T> {
        void accept(T t) throws Exception;
    }

    public static void zkTemplate(ExConsumer<ZooKeeper> zkOper) {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper("PC135:12181,FAT:12181,TROY:12181", 9999999, null);
            if (zkOper != null) zkOper.accept(zk);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void readZKTree() {
        zkTemplate(zk -> {
            Stack<String> stack = new Stack<>();
            LinkedHashMap<String, String> pathAndDatas = new LinkedHashMap<>();
            String path = "/study";
            stack.push(path);

            Stat stat = new Stat();
            do {
                String _path = stack.pop();
                if (zk.exists(_path, false) == null) {
                    System.out.printf("%s\t=>\t%s\n", _path, "not existent");
                    continue;
                }
                byte[] data = zk.getData(_path, false, stat);
                String value = data == null ? null : new String(data);
                pathAndDatas.put(_path, value);

                if (stat.getNumChildren() != 0) {
                    List<String> children = zk.getChildren(_path, false);
//                    children.stream().map((String child) -> (_path.endsWith("/") ? _path : "/") + child).forEach(System.out::println);
                    children.stream().map((String child) -> _path + (_path.endsWith("/") ? "" : "/") + child).forEach(stack::push);
                }
            } while (!stack.empty());
            pathAndDatas.entrySet().forEach(System.out::println);
        });
    }

    @Test
    public void readZookeeper() {
        zkTemplate(zk -> {
            Stat stat = new Stat();
            byte[] data = zk.getData("/", false, stat);
            System.out.println(new String(data));
            System.out.println(stat);
            System.out.println(String.format("dataLength:%d\tnumChildren:%d", stat.getDataLength(), stat.getNumChildren()));
        });
    }

    @Test
    public void createZKNode() {
        zkTemplate(zk -> {
            String path = "/study";
            Stat stat = zk.exists(path, null);
            byte[] value = "study".getBytes();

            if (stat != null) {
                stat = zk.setData(path, value, stat.getVersion() + 1);
            } else {
                zk.create(path, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zk.delete(path, -1);
        });
    }

    @Test
    public void deleteZKNode() {
        zkTemplate(zk -> {
            String path = "/study";
//            zk.delete(path, -1);
            _readZKTree(zk, "/study").entrySet().forEach(System.out::println);
            deleteZKTree(zk, path);
            _readZKTree(zk, "/study").entrySet().forEach(System.out::println);
        });
    }

    static class TreeNode {
        String path;
        String name;
        Set<TreeNode> children;
    }

    @Test
    public void testMergePaths() {
        Map<String, String> pathAndValues = new HashMap<>();
        pathAndValues.put("/study/a", "a");
        pathAndValues.put("/study/a/b/c", "c");
        pathAndValues.put("/study/a/b", "b");
        pathAndValues.put("/study/b", "b");
        pathAndValues.put("/study/b/c", "c");
        pathAndValues.put("/study/d/c", "d");
        pathAndValues.put("/study/d/c/b", "b");
        pathAndValues.put("/study/d/c/b/a", "a");

        List<String> paths = new ArrayList<>(pathAndValues.keySet());
        paths.forEach(System.out::println);
        System.out.println();
        mergePaths(paths.toArray(new String[0])).forEach(System.out::println);
    }

    @Test
    public void createZKTree() {
        zkTemplate(zk -> {
            Map<String, String> pathAndValues = new HashMap<>();
            pathAndValues.put("/study/a", "a");
            pathAndValues.put("/study/a/b/c", "c");
            pathAndValues.put("/study/a/b", "b");
            pathAndValues.put("/study/b", "b");
            pathAndValues.put("/study/b/c", "c");
            pathAndValues.put("/study/d/c", "d");
            pathAndValues.put("/study/d/c/b", "b");
            pathAndValues.put("/study/d/c/b/a", "a");

            List<String> paths = new ArrayList<>(pathAndValues.keySet());
            Collections.sort(paths);

            String lastPath = paths.get(0);
            ensurePath(zk, lastPath, null);
            createOrSet(zk, lastPath, pathAndValues.get(lastPath));
            for (int i = 1, len = paths.size(); i < len; i++) {
                String path = paths.get(i);
                int lastSep = StringUtils.lastIndexOf(path, '/');
                String _path = StringUtils.left(path, lastSep + 1);
                String prefix = StringUtils.getCommonPrefix(lastPath, _path);
                String between = StringUtils.substring(_path, prefix.length());
                ensurePath(zk, between, prefix);
                createOrSet(zk, path, pathAndValues.get(path));
                lastPath = path;
            }
            _readZKTree(zk, "/study").entrySet().forEach(System.out::println);
        });

    }
}