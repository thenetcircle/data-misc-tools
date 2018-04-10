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

import java.util.Map;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.*;

@Description(
        name = "m_add",
        value = "_FUNC_(map1, map2....) - Returns a combined map of N maps"
)
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFMapAdd extends GenericUDF {
    private transient MapObjectInspector mapInsp;
    private transient StandardMapObjectInspector retInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        super.checkArgsSize(args, 2, Integer.MAX_VALUE);

        if (ObjectInspector.Category.MAP != args[0].getCategory()) {
            throw new UDFArgumentException("m_add parameters must be all maps of the same type");
        }
        for (int i = 1, j = args.length; i < j; i++) {
            if (!compareTypes(args[0], args[i])) {
                throw new UDFArgumentException("m_add parameters must be all maps of the same type");
            }
        }
        mapInsp = (StandardMapObjectInspector) args[0];
        retInsp = (StandardMapObjectInspector) getStandardObjectInspector(mapInsp, ObjectInspectorCopyOption.JAVA);
        return retInsp;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        Object theMap = retInsp.create();

        for (DeferredObject arg : args) {
            Object obj = arg.get();
            if (obj == null) continue;
            Map eachMap = mapInsp.getMap(obj);
            for (Object entry : eachMap.entrySet()) {
                Map.Entry kv = (Map.Entry) entry;
                retInsp.put(theMap,
                        copyToStandardJavaObject(kv.getKey(), mapInsp.getMapKeyObjectInspector()),
                        copyToStandardJavaObject(kv.getValue(), mapInsp.getMapValueObjectInspector())
                );
            }
        }

        return theMap;
    }

    @Override
    public String getDisplayString(String[] args) {
        return format("m_add(%s)", StringUtils.join(args, ","));
    }
}
