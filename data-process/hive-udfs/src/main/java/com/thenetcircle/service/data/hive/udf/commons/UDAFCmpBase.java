package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.what;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.*;

@Description(name = "max_with", value = "max_with(Primative sorted, Object data)")
public class UDAFCmpBase extends AbstractGenericUDAFResolver {

    static final Logger log = LoggerFactory.getLogger(UDAFCmpBase.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        ObjectInspector[] paramOIs = info.getParameterObjectInspectors();

        if (paramOIs.length != 2) {
            throw new UDFArgumentException("Two parameters are required");
        }

        ObjectInspector sortKey = paramOIs[0];
        if (sortKey.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "key param must be primitive but " + sortKey.getTypeName() + " was passed in");
        }

        return new BaseEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] paramInfos) throws SemanticException {
        if (paramInfos.length != 2) {
            throw new UDFArgumentException("Two parameters are required");
        }
        TypeInfo ti = paramInfos[0];
        if (ti.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "Only primitive type arguments are accepted but "
                    + ti.getTypeName() + " is passed.");
        }

        return new BaseEvaluator();
    }

    public static class KeyAndDataBuf extends AbstractAggregationBuffer {
        Object key;
        Object data;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("KeyAndDataBuf{");
            sb.append("key=").append(key);
            sb.append(", data=").append(what(data));
            sb.append('}');
            return sb.toString();
        }
    }

    //    @UDFType(distinctLike=true, deterministic=true)
    public static class BaseEvaluator extends GenericUDAFEvaluator {
        transient PrimitiveObjectInspector keyOI;
        transient ObjectInspector dataOI;
        transient StructObjectInspector middleResultOI;
        StructField keyField;
        StructField dataField;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] paramOIs) throws HiveException {
            super.init(m, paramOIs);
            try {
                if (m == Mode.PARTIAL1) {
                    keyOI = (PrimitiveObjectInspector) paramOIs[0];
                    dataOI = getStandardObjectInspector(paramOIs[1], WRITABLE);

                    return ObjectInspectorFactory.getStandardStructObjectInspector(
                        Arrays.asList("key", "data"),
                        Arrays.asList(
                            getStandardObjectInspector(keyOI, WRITABLE),
                            getStandardObjectInspector(dataOI, WRITABLE))
                    );
                }

                if (m == Mode.COMPLETE) {
                    keyOI = (PrimitiveObjectInspector) paramOIs[0];
                    dataOI = paramOIs[1];

                    return dataOI;
                }

                if (m == Mode.PARTIAL2) {
                    middleResultOI = (StructObjectInspector) paramOIs[0];
                    keyField = middleResultOI.getStructFieldRef("key");
                    keyOI = (PrimitiveObjectInspector) keyField.getFieldObjectInspector();
                    dataField = middleResultOI.getStructFieldRef("data");
                    dataOI = dataField.getFieldObjectInspector();

                    return ObjectInspectorFactory.getStandardStructObjectInspector(
                        Arrays.asList("key", "data"),
                        Arrays.asList(
                            getStandardObjectInspector(keyOI, WRITABLE),
                            getStandardObjectInspector(dataOI, WRITABLE))
                    );
                }

                if (m == Mode.FINAL) {
                    middleResultOI = (StructObjectInspector) paramOIs[0];
                    keyField = middleResultOI.getStructFieldRef("key");
                    keyOI = (StringObjectInspector) keyField.getFieldObjectInspector();
                    dataField = middleResultOI.getStructFieldRef("data");
                    dataOI = dataField.getFieldObjectInspector();

                    return dataOI;
                }

                return null;
            } finally {
                log.info("BaseEvaluator.init"
                    + format("\n\tthis:\t%s", this)
                    + format("\n\tmode:\t%s", m)
                    + format("\n\tparamOIs:\t%s", StringUtils.join(paramOIs, "\n\t\t")));
            }
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() {
            return new KeyAndDataBuf();
        }

        @Override
        public void reset(AggregationBuffer agg) {
            KeyAndDataBuf kdb = (KeyAndDataBuf) agg;
            kdb.key = null;
            kdb.data = null;
        }

        protected void updateBuf(KeyAndDataBuf kdb, Object _key, Object _data) {
//            int compared = ObjectUtils.compare(primitiveJavaObject, (Comparable) kdb.key); //compare(_key, keyOI, kdb.key, keyOI);
            int compared = compare(_key, keyOI, kdb.key,
                getStandardObjectInspector(keyOI, WRITABLE)
            );
            if (compared > 0) {
                kdb.key = copyToStandardObject(_key, keyOI, WRITABLE);
                kdb.data = copyToStandardObject(_data, dataOI, WRITABLE);
            }

            Comparable primitiveJavaObject = (Comparable) keyOI.getPrimitiveJavaObject(_key);
            log.info(new StringBuilder()
                .append("\n").append(format("updateBuf: kdb: %s", kdb))
                .append("\n\t").append(format("_key: %s", Optional.of(_key).map(_p -> _p.getClass() + ":" + _p).orElse("null")))
                .append("\n\t").append(format("_data: %s", _data))
                .append("\n\t").append(format("compared(%s, %s) = %d", primitiveJavaObject, kdb.key, compared))
                .toString());
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] params) {
            updateBuf((KeyAndDataBuf) agg, params[0], params[1]);
            log.info("BaseEvaluator.iterate"
                + format("\n\tagg:\t%s", agg)
                + format("\n\tparams:\t%s", StringUtils.join(params, "\n\t\t")));
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            KeyAndDataBuf kdb = (KeyAndDataBuf) agg;
            log.info(format("\n\n%s\n\tBaseEvaluator.terminatePartial params:%s\n\n", this, agg));
            return new Object[]{kdb.key, kdb.data};
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

            KeyAndDataBuf kdb = (KeyAndDataBuf) agg;
            Object keyObj = middleResultOI.getStructFieldData(partial, keyField);
            Object dataObj = middleResultOI.getStructFieldData(partial, dataField);

            log.info(format("\n\n%s\n\n%s\n\tBaseEvaluator.merge params:\n\t%s\n\t%s\n\t%s",
                this,
                agg,
                partial,
                keyObj,
                dataObj));
            updateBuf(kdb, keyObj, dataObj);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            KeyAndDataBuf kdb = (KeyAndDataBuf) agg;
            log.info(format("\n\nBaseEvaluator.terminate:\t%s\n%s\n\n", this, agg));
            return kdb.data;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("BaseEvaluator{");
            sb.append(" keyOI=").append(keyOI);
            sb.append(", \ndataOI=").append(dataOI);
            sb.append(", \nmiddleResultOI=").append(middleResultOI);
            sb.append(", \nkeyField=").append(keyField);
            sb.append(", \ndataField=").append(dataField);
            sb.append('}');
            return sb.toString();
        }
    }
}
