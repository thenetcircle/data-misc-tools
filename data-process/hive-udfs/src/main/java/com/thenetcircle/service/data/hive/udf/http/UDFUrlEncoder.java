package com.thenetcircle.service.data.hive.udf.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.UTF_8;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(name = "url_encode",
    value = "_FUNC_(string) - encode a string into url piece")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFUrlEncoder extends GenericUDF {

    private StringObjectInspector strInsp;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
    private ObjectInspectorConverters.Converter strConverter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);
        checkArgPrimitive(args, 0);
        checkArgGroups(args, 0, inputTypes, STRING_GROUP);

        if (args[0] instanceof VoidObjectInspector) {
            strInsp = null;
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

        strInsp = (StringObjectInspector) args[0];
        strConverter = ObjectInspectorConverters.getConverter(strInsp, PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (strInsp == null || args[0] == null) return "null";
        try {
            return URLEncoder.encode((String) strConverter.convert(args[0].get()), UTF_8);
        } catch (UnsupportedEncodingException e) {
            return e.getLocalizedMessage();
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("url_encode(%s)", StringUtils.join(children, ", "));
    }
}
