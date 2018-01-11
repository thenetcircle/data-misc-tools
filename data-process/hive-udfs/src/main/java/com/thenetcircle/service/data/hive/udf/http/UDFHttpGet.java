package com.thenetcircle.service.data.hive.udf.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Map;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.*;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(name = "http_get",
        value = "_FUNC_(String url, int timeout, Map<String, String> headers) - send get request to url with headers in timeout")
@UDFType(deterministic = false, stateful = false, distinctLike = true)
public class UDFHttpGet extends GenericUDF {

    private transient StringObjectInspector urlInsp;
    private int timeout = 3000;
    private transient MapObjectInspector headersInsp = null;
    private transient RequestConfig rc = null;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 3);
        checkArgPrimitive(args, 0);
        checkArgGroups(args, 0, inputTypes, STRING_GROUP);

        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "url parameter must be string:\n\t" + args[0]);
        }
        this.urlInsp = (StringObjectInspector) args[0];

        if (args.length > 1) {
            checkArgPrimitive(args, 1);
            checkArgGroups(args, 1, inputTypes, NUMERIC_GROUP);
            timeout = getConstantIntValue(args, 1);
            rc = RequestConfig.custom()
                    .setSocketTimeout(timeout)
                    .setConnectTimeout(timeout)
                    .setConnectionRequestTimeout(timeout)
                    .build();
        }

        if (args.length > 2) {
            ObjectInspector _headerInsp = args[2];
            if (_headerInsp instanceof WritableVoidObjectInspector) {
                headersInsp = null;
            } else {
                if (!(_headerInsp instanceof MapObjectInspector)) {
                    throw new UDFArgumentTypeException(2, "header parameter must be map<string, object> or null:\n\t" + args[2]);
                }
                MapObjectInspector moi = (MapObjectInspector) _headerInsp;
                if (!(moi.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
                    throw new UDFArgumentTypeException(2, "header parameter must be map<string, object>");
                }
                headersInsp = moi;
            }
        }

        return RESULT_TYPE;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        DeferredObject arg0 = args[0];
        String urlStr = this.urlInsp.getPrimitiveJavaObject(arg0.get());
        if (StringUtils.isBlank(urlStr)) {
            return runtimeErr("url is blank");
        }

        HttpGet get = new HttpGet(urlStr);
        get.setConfig(rc);

        if (args.length > 2 && args[2] != null && headersInsp != null) {
            DeferredObject arg2 = args[2];
            Map<?, ?> headersMap = headersInsp.getMap(arg2.get());
            get.setHeaders(map2Headers(headersMap));
        }

        if (hc == null) hc = HttpClientBuilder.create().build();

        return sendAndGetHiveResult(hc, get);
    }

    private CloseableHttpClient hc = null;

    @Override
    public void close() throws IOException {
        super.close();
        if (hc != null) {
            hc.close();
            hc = null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("http_get(%s)", StringUtils.join(children, ", "));
    }

}
