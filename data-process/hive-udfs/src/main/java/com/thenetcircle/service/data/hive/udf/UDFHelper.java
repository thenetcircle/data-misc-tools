package com.thenetcircle.service.data.hive.udf;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hive.common.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.hadoop.hive.ql.exec.GroupByOperator.*;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.getConverter;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.*;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromObjectInspector;

public class UDFHelper {
    public static Set<Class> NUMERIC_CLZZ = new HashSet<>(Arrays.asList(
        Boolean.class,
        Byte.class,
        Short.class,
        Integer.class,
        Long.class,
        Float.class,
        Double.class
    ));

    public static int getSize(Object obj) {
        Class c = obj.getClass();

        if (c.isPrimitive() || NUMERIC_CLZZ.contains(c)) {
            return JavaDataModel.JAVA64_OBJECT;
        }

        if (Timestamp.class.isAssignableFrom(c)) {
            return javaObjectOverHead + javaSizePrimitiveType;
        }

        return javaSizeUnknownType;
    }

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

        List<? extends StructField> fieldRefs = soip.getAllStructFieldRefs();
        List<String> fieldNames = fieldRefs.stream().map(StructField::getFieldName).collect(Collectors.toList());
        List<ObjectInspector> fieldInsp = fieldRefs.stream().map(StructField::getFieldObjectInspector).collect(Collectors.toList());

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

//            List<TypeInfo> parameterTypeInfos = TypeInfoUtils.getParameterTypeInfos(md, params.length);
//            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo()

            for (int i = 0, j = params.length; i < j; i++) {
                Parameter param = params[i];
                Class<?> paramType = param.getType();

                ObjectInspector argObjInsp = argOIs[i];
                if (param.isVarArgs()) {
                    for (int i1 = i; i1 < j; i1++) {
                        checkPrimitiveParamAndArgOI(paramType.getComponentType(), argObjInsp, i1);
                    }
                    mb.objInspAndConverters.add(new ImmutablePair(argObjInsp,
                        getConverter(argObjInsp,
                            getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(paramType.getComponentType()).primitiveCategory)
                        )
                    ));
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

            System.out.println("mb = " + ToStringBuilder.reflectionToString(mb, ToStringStyle.MULTI_LINE_STYLE));

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
                AbstractPrimitiveJavaObjectInspector valObjInsp = getPrimitiveJavaObjectInspector(getTypeEntryFromPrimitiveJava(valClz).primitiveCategory);

                return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjInsp, valObjInsp);
            }
        }

        return null;
    }

    public final static ListTypeInfo getListTypeInfo(TypeInfo elTypeInfo) {
        ListTypeInfo lti = new ListTypeInfo();
        lti.setListElementTypeInfo(elTypeInfo);
        return lti;
    }

    public final static StructTypeInfo getStructTypeInfo(List<String> allStructFieldNames,
                                                         List<TypeInfo> allStructFieldTypeInfos) {
        StructTypeInfo sti = new StructTypeInfo();
        sti.setAllStructFieldNames(new ArrayList<String>(allStructFieldNames));
        sti.setAllStructFieldTypeInfos(new ArrayList<TypeInfo>(allStructFieldTypeInfos));
        return sti;
    }

    public static TypeInfo typeToTypeInfo(Type t) {
        if (t == Object.class) return TypeInfoFactory.unknownTypeInfo;

        if (t instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) t;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (List.class == (Class) pt.getRawType() || ArrayList.class == (Class) pt.getRawType()) {
                return getListTypeInfo(typeToTypeInfo(actualTypeArguments[0]));
            }

            if (Map.class == (Class) pt.getRawType() || HashMap.class == (Class) pt.getRawType()) {
                return getMapTypeInfo(typeToTypeInfo(actualTypeArguments[0]),
                    typeToTypeInfo(actualTypeArguments[1]));
            }

            t = pt.getRawType();
        }

        if (!(t instanceof Class)) {
            throw new RuntimeException("Hive does not understand type " + t);
        }
        Class c = (Class) t;
        if (c.isPrimitive()) {
            return getPrimitiveTypeInfoFromJavaPrimitive(c);
        }
        if (c.isArray()) {
            return getListTypeInfo(typeToTypeInfo(c.getComponentType()));
        }
        if (Date.class.isAssignableFrom(c)) {
            return TypeInfoFactory.dateTypeInfo;
        }
        if (PrimitiveObjectInspectorUtils.isPrimitiveJavaType(c)) {
            return getTypeInfoFromObjectInspector(
                getPrimitiveJavaObjectInspector(
                    getTypeEntryFromPrimitiveJavaType(c).primitiveCategory));
        }
        if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(c)) {
            return getTypeInfoFromObjectInspector(
                getPrimitiveJavaObjectInspector(
                    getTypeEntryFromPrimitiveJavaClass(c).primitiveCategory));
        }
        if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(c)) {
            return getTypeInfoFromObjectInspector(
                getPrimitiveWritableObjectInspector(
                    getTypeEntryFromPrimitiveWritableClass(c).primitiveCategory));
        }

        Field[] fields = ObjectInspectorUtils.getDeclaredNonStaticFields(c);

        List<String> fieldNameList = Arrays.stream(fields)
            .map(Field::getName)
            .collect(Collectors.toList());

        List<TypeInfo> tiList = Arrays.stream(fields)
            .map(Field::getType)
            .map(UDFHelper::typeToTypeInfo)
            .collect(Collectors.toList());

        return getStructTypeInfo(fieldNameList, tiList);
    }

    public static TypeInfo getParamentTypeInfo(Parameter param) {
        if (param == null) return null;
        return typeToTypeInfo(param.getType());
    }

    public final static String what(Object obj) {
        if (obj == null) return "[null]";
        return format("%s:\t%s", obj.getClass().getSimpleName(), obj);
    }

//    public static LazyBinaryObject toLazyBin(ObjectInspector lazyOI) {
//        if (lazyOI == null) {
//            return createLazyBinaryPrimitiveClass(writableVoidObjectInspector);
//        }
//
//        switch (lazyOI.getCategory()) {
//            case PRIMITIVE:
//                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) lazyOI;
//                return createLazyBinaryPrimitiveClass(
//                    getPrimitiveWritableObjectInspector(poi.getTypeInfo()));
//
//            case LIST:
//
//        }
//    }

    public static class _StandardListOI extends StandardListObjectInspector {
        public _StandardListOI() {
            super();
        }

        public _StandardListOI(ObjectInspector elOI) {
            super(elOI);
        }
    }

    public static class _StandardMapOI extends StandardMapObjectInspector {
        public _StandardMapOI() {
            super();
        }

        public _StandardMapOI(ObjectInspector keyOI, ObjectInspector valueOI) {
            super(keyOI, valueOI);
        }
    }

    public static class _StandardStructOI extends StandardStructObjectInspector {
        public _StandardStructOI() {
            super();
        }

        public _StandardStructOI(List<String> structFieldNames,
                                 List<ObjectInspector> structFieldObjectInspectors) {
            super(structFieldNames, structFieldObjectInspectors);
        }
    }
}
