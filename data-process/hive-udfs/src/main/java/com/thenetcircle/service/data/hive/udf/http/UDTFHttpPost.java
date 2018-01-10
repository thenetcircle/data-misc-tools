package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.commons.UDTFExt;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.*;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;


@Description(name = UDTFHttpPost.NAME,
    value = "_FUNC_(url, timeout, headers, content) - send post to url with headers in timeout")
public class UDTFHttpPost extends UDTFExt {
    public static final String NAME = "t_http_post";

    private transient StringObjectInspector urlInsp;
    private transient StringObjectInspector contentInsp;
    private int timeout = 3000;
    private transient MapObjectInspector headersInsp = null;
    private transient RequestConfig rc = null;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[4];

    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(NAME, args, 1, 3);
        checkArgPrimitive(NAME, args, 0);
        checkArgGroups(NAME, args, 0, inputTypes, STRING_GROUP);

        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "url parameter must be string:\n\t" + args[0]);
        }
        this.urlInsp = (StringObjectInspector) args[0];

        if (args.length > 1) {
            checkArgPrimitive(NAME, args, 1);
            checkArgGroups(NAME, args, 1, inputTypes, NUMERIC_GROUP);
            timeout = getConstantIntValue(NAME, args, 1);
            rc = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).build();
        }

        //headers
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

        if (args.length > 3) {
            checkArgGroups(NAME, args, 3, inputTypes, STRING_GROUP);
            ObjectInspector contentObj = args[3];
            if (!(args[0] instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(0, "content must be string");
            }
            this.contentInsp = (StringObjectInspector) contentObj;
        }
        return RESULT_TYPE;
    }

    @Override
    public Object[] evaluate(Object[] args, int start) {
        String urlStr = this.urlInsp.getPrimitiveJavaObject(args[start + 0]);
        if (StringUtils.isBlank(urlStr)) {
            return runtimeErr("url is blank");
        }

        HttpPost post = new HttpPost(urlStr);
        post.setConfig(rc);

        if (args.length > start + 2 && args[start + 2] != null && headersInsp != null) {
            Map<?, ?> headersMap = headersInsp.getMap(args[start + 2]);
            post.setHeaders(map2Headers(headersMap));
        }

        if (args.length > start + 3) {
            String content = contentInsp.getPrimitiveJavaObject(args[start + 3]);
            try {
                post.setEntity(new StringEntity(content));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return runtimeErr(e);
            }
        }

        return sendAndGetHiveResult(hc, post);
    }

    private transient CloseableHttpClient hc = HttpClientBuilder.create().build();

    @Override
    public void close() throws HiveException {
        HttpHelper.close(hc);
        hc = null;
    }

}
