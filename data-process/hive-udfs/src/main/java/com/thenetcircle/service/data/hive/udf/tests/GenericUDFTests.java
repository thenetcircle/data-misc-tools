package com.thenetcircle.service.data.hive.udf.tests;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.envProbe;
import static com.thenetcircle.service.data.hive.udf.UDFHelper.stackTraces;
import static java.lang.String.format;

@Description(name = "gudf_tests",
    value = "_FUNC_(obj) tells some info of the parameter")
@UDFType(deterministic = false, stateful = false, distinctLike = true)
public class GenericUDFTests extends GenericUDF {
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    public GenericUDFTests() {
        System.out.printf("\n%s created\n%s\n\n", this, stackTraces(2));
    }

    @Override
    public void configure(MapredContext context) {
        MapredContext.close();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        System.out.printf("\n%s initialized\n%s\n\n", this.toString(), stackTraces(2));
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        System.out.printf("\n%s evaluate\n%s\n\n", this.toString(), stackTraces(20));
        List<Object> list = new ArrayList();
        list.add(envProbe());

        for (DeferredObject arg : args) {
            String str = ToStringBuilder.reflectionToString(arg.get());
            System.out.printf("%d\t%s\n", list.size(), str);
            list.add(str);
        }
        return list.toArray();
    }

    @Override
    public void close() {
        System.out.printf("\n%s is closing\n%s\n\n", this, StringUtils.join(Thread.currentThread().getStackTrace(), "\n"));
//        throw new RuntimeException("test close");
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("gudf_tests(%s)", StringUtils.join(children, ", "));
    }
}
