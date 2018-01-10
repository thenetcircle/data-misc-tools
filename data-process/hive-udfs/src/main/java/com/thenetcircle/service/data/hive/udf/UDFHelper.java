package com.thenetcircle.service.data.hive.udf;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.io.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hive.common.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.getConverter;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.*;

public class UDFHelper {

    public static PrimitiveTypeEntry getTypeFor(Class<?> retType)
        throws UDFArgumentException {
        PrimitiveTypeEntry entry =
            getTypeEntryFromPrimitiveJavaType(retType);

        if (entry == null) {
            entry = getTypeEntryFromPrimitiveJavaClass(retType);
        }

        if (entry == null) {
            throw new UDFArgumentException("Invalid return type " + retType);
        }

        return entry;
    }

    public static String stackTraces(int n) {
        return StringUtils.join(ArrayUtils.subarray(Thread.currentThread().getStackTrace(), 1, n), "\n");
    }

    public static Map deferedObj2Map(GenericUDF.DeferredObject deferredObj, MapObjectInspector mapInsp, StandardMapObjectInspector retInsp) throws HiveException {


        if (deferredObj == null || mapInsp == null) return new HashMap();

        Object _obj = deferredObj.get();
        return obj2Map(_obj, mapInsp, retInsp);
    }

    public static Map obj2Map(Object _obj, MapObjectInspector mapInsp, StandardMapObjectInspector retInsp) {
        Map map = new HashMap();
        Map eachMap = mapInsp.getMap(_obj);

        if (eachMap == null) return map;

        for (Object entry : eachMap.entrySet()) {
            Map.Entry kv = (Map.Entry) entry;
            retInsp.put(map,
                ObjectInspectorUtils.copyToStandardJavaObject(kv.getKey(), mapInsp.getMapKeyObjectInspector()),
                ObjectInspectorUtils.copyToStandardJavaObject(kv.getValue(), mapInsp.getMapValueObjectInspector())
            );
        }
        return map;
    }

    public static void checkArgsSize(String funcName,
                                     ObjectInspector[] args,
                                     int min,
                                     int max) throws UDFArgumentLengthException {
        if (args.length >= min && args.length <= max) return;
        throw new UDFArgumentLengthException(
            format("%s requires %d..%d argument(s), got %d", funcName, min, max, args.length)
        );
    }

    public static void checkArgPrimitive(String funcName, ObjectInspector[] arguments, int i)
        throws UDFArgumentTypeException {
        ObjectInspector.Category oiCat = arguments[i].getCategory();
        if (oiCat != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(i, funcName + " only takes primitive types as "
                + getArgOrder(i) + " argument, got " + oiCat);
        }
    }

    static String getArgOrder(int i) {
        i++;
        switch (i % 100) {
            case 11:
            case 12:
            case 13:
                return i + "th";
            default:
                return i + ORDINAL_SUFFIXES[i % 10];
        }
    }

    private static final String[] ORDINAL_SUFFIXES = new String[]{"th", "st", "nd", "rd", "th",
        "th", "th", "th", "th", "th"};

    public static void checkArgGroups(String funcName,
                                      ObjectInspector[] args,
                                      int i,
                                      PrimitiveObjectInspector.PrimitiveCategory[] inputTypes,
                                      PrimitiveGrouping... grps) throws UDFArgumentTypeException {

        PrimitiveObjectInspector.PrimitiveCategory inputType = ((PrimitiveObjectInspector) args[i]).getPrimitiveCategory();

        for (PrimitiveGrouping grp : grps) {
            if (getPrimitiveGrouping(inputType) == grp) {
                inputTypes[i] = inputType;
                return;
            }
        }
        // build error message
        StringBuilder sb = new StringBuilder();
        sb.append(funcName);
        sb.append(" only takes ");
        sb.append(grps[0]);
        for (int j = 1; j < grps.length; j++) {
            sb.append(", ");
            sb.append(grps[j]);
        }
        sb.append(" types as ");
        sb.append(getArgOrder(i));
        sb.append(" argument, got ");
        sb.append(inputType);
        throw new UDFArgumentTypeException(i, sb.toString());
    }

    public static void obtainStringConverter(ObjectInspector[] arguments, int i,
                                             PrimitiveObjectInspector.PrimitiveCategory[] inputTypes,
                                             Converter[] converters) throws UDFArgumentTypeException {

        PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
        PrimitiveObjectInspector.PrimitiveCategory inputType = inOi.getPrimitiveCategory();

        Converter converter = getConverter(
            arguments[i],
            PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        converters[i] = converter;
        inputTypes[i] = inputType;
    }


    public static Date getDateValue(String funcName,
                                    GenericUDF.DeferredObject[] args,
                                    int i,
                                    PrimitiveObjectInspector.PrimitiveCategory[] inputTypes,
                                    Converter[] converters) throws HiveException {
        GenericUDF.DeferredObject arg = args[i];
        PrimitiveObjectInspector.PrimitiveCategory inputType = inputTypes[i];
        Converter converter = converters[i];
        return getDate(funcName, arg, inputType, converter);
    }

    public static Date getDate(String funcName,
                               GenericUDF.DeferredObject arg,
                               PrimitiveObjectInspector.PrimitiveCategory inputType,
                               Converter converter) throws HiveException {
        Object obj;
        if ((obj = arg.get()) == null) {
            return null;
        }

        Date date;
        switch (inputType) {
            case STRING:
            case VARCHAR:
            case CHAR:
                String dateStr = converter.convert(obj).toString();
                try {
                    date = DateUtils.getDateFormat().parse(dateStr);
                } catch (ParseException e) {
                    return null;
                }
                break;
            case TIMESTAMP:
            case DATE:
                Object writableValue = converter.convert(obj);
                date = ((DateWritable) writableValue).get();
                break;
            default:
                throw new UDFArgumentTypeException(0, funcName
                    + " only takes STRING_GROUP and DATE_GROUP types, got " + inputType);
        }
        return date;
    }

    public static Integer getConstantIntValue(String funcName, ObjectInspector[] args, int i) throws UDFArgumentTypeException {
        Object constValue = ((ConstantObjectInspector) args[i]).getWritableConstantValue();
        if (constValue == null) {
            return null;
        }

        int v;
        if (constValue instanceof IntWritable) {
            v = ((IntWritable) constValue).get();
        } else if (constValue instanceof ShortWritable) {
            v = ((ShortWritable) constValue).get();
        } else {
            if (!(constValue instanceof ByteWritable)) {
                throw new UDFArgumentTypeException(i, funcName + " only takes INT/SHORT/BYTE types as " + getArgOrder(i) + " argument, got " + constValue.getClass());
            }
            v = ((ByteWritable) constValue).get();
        }

        return v;
    }

    public static <T> Object[] map(T[] array, Function<T, Object> mapper) {
        return Stream.of(array).map(mapper).toArray();
    }

    public static ObjectInspector checkContext(ObjectInspector input) throws UDFArgumentTypeException {
        if (input == null)
            throw new UDFArgumentTypeException(0, "context parameter must be a struct! ObjectInspeector is null!!");

        if (input instanceof VoidObjectInspector) {
            return null;
        }

        return input;
    }

    public static StructObjectInspector addContextToStructInsp(StructObjectInspector soip, ObjectInspector oip) {
        if (soip == null || oip == null) return null;

        List<String> fieldNames = soip.getAllStructFieldRefs().stream().map(StructField::getFieldName).collect(Collectors.toList());
        List<ObjectInspector> fieldInsp = soip.getAllStructFieldRefs().stream().map(StructField::getFieldObjectInspector).collect(Collectors.toList());

        fieldNames.add("ctx");
        fieldInsp.add(oip);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInsp);
    }

    private static transient final Logger log = LoggerFactory.getLogger(UDFHelper.class);

    static {
        log.info("\n\nTNC UDFs are loaded!\n\n");
        System.out.println("\n\nTNC UDFs are loaded!\n\n");
    }

    public static Object deferedObjGet(GenericUDF.DeferredObject defObj, Object defaultValue) {
        try {
            return defObj.get();
        } catch (HiveException e) {
            e.printStackTrace();
            return defaultValue;
        }
    }

    public static String envProbe() {
        return format("{host:'%s',thread:'%s'}", NetUtils.getHostname(), Thread.currentThread());
    }

    public static Writable ret2ObjInsp(Writable returnObj, PrimitiveObjectInspector returnOI, Object result) throws HiveException {
        switch (returnOI.getPrimitiveCategory()) {
            case VOID:
                return null;
            case BOOLEAN:
                ((BooleanWritable) returnObj).set((Boolean) result);
                return returnObj;
            case BYTE:
                ((ByteWritable) returnObj).set((Byte) result);
                return returnObj;
            case SHORT:
                ((ShortWritable) returnObj).set((Short) result);
                return returnObj;
            case INT:
                ((IntWritable) returnObj).set((Integer) result);
                return returnObj;
            case LONG:
                ((LongWritable) returnObj).set((Long) result);
                return returnObj;
            case FLOAT:
                ((FloatWritable) returnObj).set((Float) result);
                return returnObj;
            case DOUBLE:
                ((DoubleWritable) returnObj).set((Double) result);
                return returnObj;
            case STRING:
                ((Text) returnObj).set((String) result);
                return returnObj;
            case TIMESTAMP:
                ((TimestampWritable) returnObj).set((Timestamp) result);
                return returnObj;
            case BINARY:
                ((BytesWritable) returnObj).set((byte[]) result, 0, ((byte[]) result).length);
                return returnObj;
            case DECIMAL:
                ((HiveDecimalWritable) returnObj).set((HiveDecimal) result);
                return returnObj;
        }
        throw new HiveException("Invalid type " + returnOI.getPrimitiveCategory());
    }

    public static class PrimitiveMethodBridge {
        public StructObjectInspector retObjInsp = null;
        public List<Pair<ObjectInspector, Converter>> objInspAndConverters = new ArrayList<>();
        public Class clz;
        public Method method;
    }

    public static void checkPrimitiveParamAndArgOI(Class<?> paramType, ObjectInspector argObjInsp, int i) throws UDFArgumentException {
        PrimitiveObjectInspector primitiveInsp = (PrimitiveObjectInspector) argObjInsp;
        PrimitiveGrouping argPrimitiveGroup = getPrimitiveGrouping(primitiveInsp.getPrimitiveCategory());

        if (PrimitiveGrouping.VOID_GROUP == argPrimitiveGroup && paramType.isPrimitive()) {
            throw new UDFArgumentTypeException(i, format("%dth parameter is expected to be %s, but found %s", i, paramType, argPrimitiveGroup));
        }

        PrimitiveTypeEntry typeEntry = getTypeEntryFromPrimitiveJava(paramType);
        PrimitiveGrouping paramGroup = getPrimitiveGrouping(typeEntry.primitiveCategory);
        if (typeEntry == null) {
            throw new UDFArgumentTypeException(i, format("%dth parameter is expected to be %s, but found %s", i, paramType, argPrimitiveGroup));
        }

        if (!argPrimitiveGroup.equals(paramGroup)) {
            throw new UDFArgumentTypeException(i, format("%dth parameter is expected to be %s, but found %s", i, paramGroup, argPrimitiveGroup));
        }
    }

    public static PrimitiveMethodBridge getMethodBridge(Class clz, Method md, ObjectInspector[] argOIs) throws UDFArgumentException {
        if (md == null)
            throw new UDFArgumentException(format("class: %s or methodName: %s is null", clz, md));

        try {
            Parameter[] params = md.getParameters();

            Parameter lastParam = params[params.length - 1];
            if (!lastParam.isVarArgs()) {
                if (argOIs.length < params.length) {
                    throw new UDFArgumentLengthException(format("parameter list mismatches\n%s\n%s", md, StringUtils.join(argOIs, "\t")));
                }
            }

            PrimitiveMethodBridge mb = new PrimitiveMethodBridge();
            mb.retObjInsp = ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList("ret"),
                Arrays.asList(retType2ObjInsp(md)));

            mb.clz = clz;
            mb.method = md;

            for (int i = 0, j = params.length; i < j; i++) {
                Parameter param = params[i];
                Class<?> paramType = param.getType();

                ObjectInspector argObjInsp = argOIs[i];
                if (param.isVarArgs()) {
                    for (int i1 = i; i1 < j; i1 ++) {
                        checkPrimitiveParamAndArgOI(paramType, argObjInsp, i1);
                    }
                    break;
                }

                if (ObjectInspector.Category.PRIMITIVE == argObjInsp.getCategory()) {
                    checkPrimitiveParamAndArgOI(paramType, argObjInsp, i);

                    mb.objInspAndConverters.add(new ImmutablePair(argObjInsp,
                        getConverter(argObjInsp,
                            getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(paramType).primitiveCategory)
                        )
                    ));

                } else if (ObjectInspector.Category.LIST == argObjInsp.getCategory()) {
                    if (!(paramType.isArray() || param.isVarArgs() || Collection.class.isAssignableFrom(paramType))) {
                        throw new UDFArgumentTypeException(i, format("%dth parameter is expected to be %s, but found %s", i, paramType, argObjInsp));
                    }

                    ListObjectInspector listInsp = (ListObjectInspector) argObjInsp;
                    ObjectInspector elementInsp = listInsp.getListElementObjectInspector();
                    if (param.isVarArgs()) {
                        checkPrimitiveParamAndArgOI(paramType, elementInsp, i);
                        mb.objInspAndConverters.add(new ImmutablePair(argObjInsp,
                            getConverter(argObjInsp,
                                ObjectInspectorFactory.getStandardListObjectInspector(
                                    getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(paramType).primitiveCategory))
                            )));
                    } else if (paramType.isArray()) {
                        Class<?> compType = paramType.getComponentType();
                        checkPrimitiveParamAndArgOI(compType, elementInsp, i);

                        mb.objInspAndConverters.add(new ImmutablePair(argObjInsp,
                            getConverter(argObjInsp,
                                ObjectInspectorFactory.getStandardListObjectInspector(
                                    getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(compType).primitiveCategory))
                            )));
                    }
                    //TODO list/set/map?

                }


            }

            return mb;
        } catch (Exception e) {
            throw new UDFArgumentException(e);
        }
    }

    public static ObjectInspector clz2ObjInsp(Class clz) {
        if (clz == null || Void.class.equals(clz)) return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        if (clz.isPrimitive() || Date.class.isAssignableFrom(clz)) {
            return getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(clz).primitiveCategory);
        }

        if (clz.isArray()) {
            return ObjectInspectorFactory.getStandardListObjectInspector(
                getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(clz.getComponentType()).primitiveCategory));
        }

        return null;
    }

    public static ObjectInspector retType2ObjInsp(Method md) {
        Class clz = md.getReturnType();
        if (clz == null || Void.class.equals(clz)) return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;


        PrimitiveTypeEntry typeEntry = getTypeEntryFromPrimitiveJava(clz);
        if (typeEntry != null)
            return getPrimitiveJavaObjectInspector(typeEntry.primitiveCategory);

        if (clz.isArray()) {
            return ObjectInspectorFactory.getStandardListObjectInspector(
                getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(clz.getComponentType()).primitiveCategory));
        }

        if (Collection.class.isAssignableFrom(clz)) {
            Type genericRetType = md.getGenericReturnType();
            if (genericRetType instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) genericRetType;
                Type[] actualTypeArgs = parameterizedType.getActualTypeArguments();
                if (ArrayUtils.isEmpty(actualTypeArgs)) return null;
                Type elementType = actualTypeArgs[0];
                if (elementType instanceof Class) {
                    Class elementClz = (Class) elementType;
                    PrimitiveTypeEntry _typeEntry = getTypeEntryFromPrimitiveJava(elementClz);
                    if (_typeEntry == null) return null;
                    AbstractPrimitiveJavaObjectInspector elementObjInsp = getPrimitiveJavaObjectInspector(_typeEntry.primitiveCategory);
                    return elementObjInsp != null ? ObjectInspectorFactory.getStandardListObjectInspector(elementObjInsp) : null;
                }
            }
        }

        if (Map.class.isAssignableFrom(clz)) {
            Type genericRetType = md.getGenericReturnType();
            if (genericRetType instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) genericRetType;
                Type[] actualTypeArgs = parameterizedType.getActualTypeArguments();
                if (ArrayUtils.isEmpty(actualTypeArgs)) return null;

                Type keyType = actualTypeArgs[0];
                Type valType = actualTypeArgs[0];
                if (!(keyType instanceof Class && valType instanceof Class)) {
                    return null;
                }
                Class keyClz = (Class) keyType;
                AbstractPrimitiveJavaObjectInspector keyObjInsp = getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(keyClz).primitiveCategory);
                Class valClz = (Class) valType;
                AbstractPrimitiveJavaObjectInspector valObjInsp = getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(keyClz).primitiveCategory);

                return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjInsp, valObjInsp);
            }
        }

        return null;
    }


}
