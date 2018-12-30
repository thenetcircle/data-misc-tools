package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;

public interface HiveJdbcBridgeUtils {
    static Object getValue(Writable w) {
        if (w == null) return null;
        Class<? extends Writable> wClz = w.getClass();
        PrimitiveObjectInspector poi = getPrimitiveObjectInspectorFromClass(wClz);
        return poi.getPrimitiveJavaObject(w);
    }

    static int toSqlType(Class<?> clazz) throws IOException {
        if (clazz == String.class || clazz == StringBuilder.class || clazz == StringBuffer.class) return Types.VARCHAR;
        if (clazz == Integer.class) return Types.INTEGER;
        if (clazz == Boolean.class) return Types.BOOLEAN;
        if (clazz == Date.class) return Types.DATE;
        if (clazz == Timestamp.class) return Types.TIMESTAMP;
        if (clazz == Float.class) return Types.FLOAT;
        if (clazz == Double.class) return Types.DOUBLE;
        if (clazz == Byte.class) return Types.TINYINT;
        if (clazz == Short.class) return Types.SMALLINT;
        if (clazz == Long.class) return Types.BIGINT;
        if (clazz == byte[].class) return Types.BINARY;
        if (clazz.isArray()) return Types.ARRAY;

        throw new IOException("Cannot resolve SqlType for + " + clazz.getName());
    }

    static int hiveTypeToSqlType(String hiveType) throws SerDeException {
        final String lctype = hiveType.toLowerCase();
        switch (lctype) {
            case "int":
                return Types.INTEGER;
            case "char":
            case "varchar":
            case "string":
                return Types.VARCHAR;
            case "timestamp":
                return Types.TIMESTAMP;
            case "date":
            case "datetime":
                return Types.DATE;
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "boolean":
                return Types.BOOLEAN;
            case "tinyint":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "binary":
                return Types.BINARY;
            case "bigdecimal":
            case "decimal":
                return Types.DECIMAL;
            default:
                if (lctype.startsWith("array")) return Types.ARRAY;
        }
        throw new SerDeException("Unrecognized column type: " + hiveType);
    }

    static String getTypeName(String hiveType) {
        StringBuilder sb = new StringBuilder();
        for (char c : hiveType.toCharArray()) {
            if (CharUtils.isAsciiAlphaLower(c)) sb.append(c);
        }
        return sb.toString();
    }

    static int[] hiveTypesToSqlTypes(String[] hiveTypes)
        throws SerDeException {
        final int[] result = new int[hiveTypes.length];
        for (int i = 0; i < hiveTypes.length; i++) {
            String hiveType = getTypeName(StringUtils.lowerCase(hiveTypes[i]));
            result[i] = hiveTypeToSqlType(hiveType);
        }
        return result;
    }

    static ObjectInspector getObjectInspector(int sqlType,
                                              String hiveType) throws SerDeException {
        switch (sqlType) {
            case Types.CHAR:
            case Types.BLOB:
            case Types.VARCHAR:
                return javaStringObjectInspector;
            case Types.TINYINT:
                return javaByteObjectInspector;
            case Types.SMALLINT:
                return javaShortObjectInspector;
            case Types.INTEGER:
                return javaIntObjectInspector;
            case Types.TIMESTAMP:
                return javaTimestampObjectInspector;
            case Types.DATE:
                return javaDateObjectInspector;
            case Types.FLOAT:
                return javaFloatObjectInspector;
            case Types.DOUBLE:
                return javaDoubleObjectInspector;
            case Types.BOOLEAN:
                return javaBooleanObjectInspector;
            case Types.BIGINT:
                return javaLongObjectInspector;
            case Types.BINARY:
                return javaByteArrayObjectInspector;
            case Types.DECIMAL:
                return new JavaHiveDecimalObjectInspector();
            case Types.ARRAY:
                String hiveElemType = hiveType.substring(hiveType.indexOf('<') + 1,
                    hiveType.indexOf('>')).trim();
                int sqlElemType = hiveTypeToSqlType(hiveElemType);
                ObjectInspector listElementOI = getObjectInspector(sqlElemType, hiveElemType);
                return getStandardListObjectInspector(listElementOI);
            default:
                throw new SerDeException("Cannot find getObjectInspector for: " + hiveType);
        }
    }

    static Writable deparseObject(Object field, ObjectInspector fieldOI) throws SerDeException {
        if (field == null) return NullWritable.get();
        if (field instanceof Writable) return (Writable) field;

        switch (fieldOI.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector oi = (PrimitiveObjectInspector) fieldOI;
                return (Writable) oi.getPrimitiveWritableObject(field);
            }
//            case LIST: {
//                ListObjectInspector listOI = (ListObjectInspector) fieldOI;
//                List<?> elements = listOI.getList(field);
//                List<Object> list = new ArrayList<Object>(elements.size());
//                ObjectInspector elemOI = listOI.getListElementObjectInspector();
//                for (Object elem : elements) {
//                    Object o = deparseObject(elem, elemOI);
//                    list.add(o);
//                }
//                return list;
//            }
            default:
                throw new SerDeException("Only supports primitive and list, Unexpected fieldOI: " + fieldOI);
        }
    }

    static Object readObject(DataInput in, int sqlType) throws IOException {
        switch (sqlType) {
            case Types.CHAR:
            case Types.BLOB:
            case Types.VARCHAR:
                return in.readUTF();
            case Types.INTEGER:
                return Integer.valueOf(in.readInt());
            case Types.TIMESTAMP:
                return new Timestamp(in.readLong());
            case Types.DATE:
                return new Date(in.readLong());
            case Types.FLOAT:
                return Float.valueOf(in.readFloat());
            case Types.DOUBLE:
                return Double.valueOf(in.readDouble());
            case Types.BOOLEAN:
                return Boolean.valueOf(in.readBoolean());
            case Types.TINYINT:
                return Byte.valueOf(in.readByte());
            case Types.SMALLINT:
                return Short.valueOf(in.readShort());
            case Types.BIGINT:
                return Long.valueOf(in.readLong());
            case Types.BINARY: {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                return b;
            }
            case Types.ARRAY: {
                int size = in.readInt();
                if (size == 0) {
                    return Collections.emptyList();
                }
                int elemType = in.readInt();
                Object[] a = new Object[size];
                for (int i = 0; i < size; i++) {
                    Object o = readObject(in, elemType);
                    a[i] = o;
                }
                return Arrays.asList(a);
            }
            default:
                throw new IOException("Cannot read Object for type: " + sqlType);
        }
    }

    static void writeObject(Object obj, int sqlType, DataOutput out)
        throws IOException {
        switch (sqlType) {
            case Types.VARCHAR: {
                String s = obj.toString();
                out.writeUTF(s);
                return;
            }
            case Types.INTEGER: {
                Integer i = (Integer) obj;
                out.writeInt(i.intValue());
                return;
            }
            case Types.TIMESTAMP: {
                Timestamp time = (Timestamp) obj;
                out.writeLong(time.getTime());
                return;
            }
            case Types.DATE: {
                Date d = (Date) obj;
                out.writeLong(d.getTime());
                return;
            }
            case Types.FLOAT: {
                Float f = (Float) obj;
                out.writeFloat(f.floatValue());
                return;
            }
            case Types.DOUBLE: {
                Double d = (Double) obj;
                out.writeDouble(d.doubleValue());
                return;
            }
            case Types.BOOLEAN: {
                Boolean b = (Boolean) obj;
                out.writeBoolean(b.booleanValue());
                return;
            }
            case Types.TINYINT: {
                Byte b = (Byte) obj;
                out.writeByte(b.intValue());
                return;
            }
            case Types.SMALLINT: {
                Short s = (Short) obj;
                out.writeShort(s.shortValue());
                return;
            }
            case Types.BIGINT: {
                Long l = (Long) obj;
                out.writeLong(l.longValue());
                return;
            }
            case Types.BINARY: {
                byte[] b = (byte[]) obj;
                out.writeInt(b.length);
                out.write(b);
                return;
            }
            case Types.ARRAY: {
                List<?> list = (List<?>) obj;
                int size = list.size();
                out.writeInt(size);
                if (size > 0) {
                    Object firstElem = list.get(0);
                    Class<?> clazz = firstElem.getClass();
                    int elemSqlType = toSqlType(clazz);
                    out.writeInt(elemSqlType);
                    for (Object e : list) {
                        writeObject(e, elemSqlType, out);
                    }
                }
                return;
            }
            default:
                throw new IOException("Cannot write Object '" + obj.getClass().getSimpleName() + "' as type: " + sqlType);
        }
    }
}
