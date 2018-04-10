package com.thenetcircle.service.data.hive.udf.tests;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static java.lang.String.format;

@Description(name = "udtf_tests",
    extended = "_FUNC_(a) tests parameters and lifecycle of GenericUDTF")
public class UDTFTests extends GenericUDTF {

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    public UDTFTests() {
        System.out.printf("\n%s created\n%s\n\n", this, stackTraces(20));
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
        throws UDFArgumentException {

        System.out.printf("\n%s initialized\n%s\n\n", this, stackTraces(20));

        return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList("result"),
            Arrays.asList(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    }

    public static String testObj(Object arg) {
        if (arg == null) return null;

        if (arg instanceof Collection) {
            Collection argCol = (Collection) arg;
            return
                format("%s[%s]", argCol.getClass().getSimpleName(),
                    StringUtils.join(argCol.stream().map(el -> testObj(el)).toArray(String[]::new), ", "));
        }

        if (arg instanceof Map) {
            Map argMap = (Map) arg;
            return format("%s[%s]", argMap.getClass().getSimpleName(),
                StringUtils.join(argMap.entrySet().stream().map(el -> testObj(el)).toArray(String[]::new), "\n"));
        }

        if (arg.getClass().isArray()) {
            Object[] array = (Object[]) arg;
            return format("%s[%s]", array.getClass().getSimpleName(),
                StringUtils.join(map(array, UDTFTests::testObj), ", "));
        }

        return ToStringBuilder.reflectionToString(arg, ToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        System.out.printf("\n%s records\n%s\n\n", this, stackTraces(20));

        String row = StringUtils.join(Stream.of(args).map(UDTFTests::testObj).toArray(String[]::new), "\t");

        System.out.printf("\n\t%s records %d args\n%s\n\n", this, args.length, row);

        forward(envProbe() + row);
    }

    @Override
    public void close() throws HiveException {
        System.out.printf("\n%s closeJedisPool\n%s\n\n", this, stackTraces(20));
    }
}
