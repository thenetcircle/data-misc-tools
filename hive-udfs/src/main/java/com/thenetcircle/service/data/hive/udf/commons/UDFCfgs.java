package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(
    name = "cfgs",
    value = "_FUNC_(key_regex_pattern:string, value_regex_pattern:string) - return variables whose keys or values match the regex pattern")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFCfgs extends GenericUDF {
    public static final StandardMapObjectInspector CFGS_INSP = getStandardMapObjectInspector(
        javaStringObjectInspector,
        javaStringObjectInspector
    );
    private transient StringObjectInspector patternStrInsp;
    private transient StandardMapObjectInspector retInsp;
    private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 0, 1);
        if (args.length == 0) {
            return CFGS_INSP;
        }

        checkArgPrimitive(args, 0);
        checkArgGroups(args, 0, inputTypes, STRING_GROUP);
        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Pattern parameter must be String:\n\t" + args[0]);
        }
        patternStrInsp = (StringObjectInspector) args[0];
        return CFGS_INSP;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        LinkedHashMap<String, String> reMap = new LinkedHashMap<>();

        DeferredObject keyPatternArg = args[0];
        String keyPatternStr = patternStrInsp.getPrimitiveJavaObject(keyPatternArg);

        DeferredObject valuePatternArg = args[1];
        String valuePatternStr = patternStrInsp.getPrimitiveJavaObject(valuePatternArg);

        SessionState ss = SessionState.get();
        Set<Map.Entry<Object, Object>> entries = ss.getConf().getAllProperties().entrySet();

        Pattern kp = StringUtils.isNoneBlank(keyPatternStr) ? Pattern.compile(keyPatternStr) : null;
        Pattern vp = StringUtils.isNoneBlank(valuePatternStr) ? Pattern.compile(valuePatternStr) : null;
        for (Map.Entry<Object, Object> en : entries) {
            String keyStr = en.getKey().toString();
            String valStr = String.valueOf(en.getValue());
            if ((kp == null || kp.matcher(keyStr).find())
                && (vp == null || vp.matcher(valStr).find())) {
                reMap.put(keyStr, valStr);
            }
        }

        return reMap;
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("cfgs(%s)", StringUtils.join(children, ", "));
    }
}
