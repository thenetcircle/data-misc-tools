package com.thenetcircle.service.data.hive.udf.commons;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgsSize;

public abstract class UDTFExt extends GenericUDTF {
    protected String funcName = null;

    public StructObjectInspector initialize(ObjectInspector[] argOIs)
        throws UDFArgumentException {

        checkArgsSize(funcName, argOIs, 1, 100);

        ObjectInspector[] _argOIs = ArrayUtils.subarray(argOIs, 1, argOIs.length);

        StructObjectInspector _retInsp = _initialize(_argOIs);

        return UDFHelper.addContextToStructInsp(_retInsp, argOIs[0]);
    }

    public abstract StructObjectInspector _initialize(ObjectInspector[] argOIs) throws UDFArgumentException;

    public abstract Object[] evaluate(Object[] _args, int start) throws HiveException;

    @Override
    public void process(Object[] args) throws HiveException {
        Object[] results = ArrayUtils.add(evaluate(args, 1), args[0]);
        forward(results);
    }
}
