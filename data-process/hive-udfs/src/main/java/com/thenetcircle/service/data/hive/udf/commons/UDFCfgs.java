package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(
    name = "cfgs",
    value = "_FUNC_(regex:string) - return cfgs whose keys match the regex pattern")
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
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("cfgs(%s)", StringUtils.join(children, ", "));
    }
}
