package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.getStandardObjectInspector;

@Description(
    name = "m_remove",
    value = "_FUNC_(map1, key1, key2....) - Returns a map with keys removed"
)
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFMapRemove extends GenericUDF {
    private transient MapObjectInspector mapInsp;
    private transient StandardStructObjectInspector retInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        super.checkArgsSize(args, 2, Integer.MAX_VALUE);

        if (ObjectInspector.Category.MAP != args[0].getCategory()) {
            throw new UDFArgumentException("m_add parameters must be all maps of the same type");
        }
        mapInsp = (StandardMapObjectInspector) args[0];
        retInsp = getStandardStructObjectInspector(
            Arrays.asList("result", "removed"),
            Arrays.asList(getStandardObjectInspector(mapInsp, ObjectInspectorCopyOption.JAVA),
                getStandardObjectInspector(mapInsp, ObjectInspectorCopyOption.JAVA)));
        return retInsp;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        Map theMap = mapInsp.getMap(args[0]);
        Map removed = new HashMap();
        for (int i = 1, j = args.length; i < j; i++) {
            Object key = args[i];
            removed.put(key, theMap.remove(key));
        }

        return new Object[]{theMap, removed};
    }

    @Override
    public String getDisplayString(String[] args) {
        return format("m_remove(%s)", StringUtils.join(args, ","));
    }
}
