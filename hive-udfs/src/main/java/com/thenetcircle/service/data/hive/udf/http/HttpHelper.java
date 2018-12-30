package com.thenetcircle.service.data.hive.udf.http;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

public class HttpHelper {
    public static Map<String, String> headers2Map(Header... headers) {
        Map<String, String> re = new HashMap<>();
        if (ArrayUtils.isEmpty(headers)) return re;
        for (Header header : headers) {
            re.put(header.getName(), header.getValue());
        }
        return re;
    }

    public static Header[] map2Headers(Map<?, ?> map) {
        if (MapUtils.isEmpty(map)) return null;
        return map.entrySet().stream()
            .map(en -> new BasicHeader(String.valueOf(en.getKey()), String.valueOf(en.getValue())))
            .toArray(Header[]::new);
    }

    public static final List<String> RESULT_FIELDS = Arrays.asList("code", "headers", "content");
    public static final List<ObjectInspector> RESULT_FIELD_INSPECTORS = Arrays.asList(
        javaIntObjectInspector,
        getStandardMapObjectInspector(
            javaStringObjectInspector,
            javaStringObjectInspector),
        javaStringObjectInspector);

    public static final StandardStructObjectInspector RESULT_TYPE = ObjectInspectorFactory.getStandardStructObjectInspector(
        RESULT_FIELDS,
        RESULT_FIELD_INSPECTORS);

    public static Object[] runtimeErr(String errMsg) {
        return new Object[]{-1, null, errMsg};
    }

    public static Object[] runtimeErr(Throwable e) {
        return new Object[]{-1, null, e.toString()};
    }

    static void close(CloseableHttpClient hc) throws HiveException {
        if (hc == null) return;
        try {
            hc.close();
        } catch (IOException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
            throw new HiveException(e);
        }
    }

    static Object[] sendAndGetHiveResult(HttpClient hc, HttpUriRequest req) {
        try {
            HttpResponse resp = hc.execute(req);
            return new Object[]{
                resp.getStatusLine().getStatusCode(),
                headers2Map(resp.getAllHeaders()),
                EntityUtils.toString(resp.getEntity())};
        } catch (IOException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
            return runtimeErr(e);
        }
    }

    public static final String UTF_8 = "UTF-8";

    public static HttpGet reqGet(String urlStr, Map<String, String> headersMap) {
        HttpGet get = new HttpGet(urlStr);
        get.setHeaders(map2Headers(headersMap));
        return get;
    }

    public static HttpPost reqPost(String urlStr, Map<String, String> headersMap, String content) {
        HttpPost post = new HttpPost(urlStr);
        post.setHeaders(map2Headers(headersMap));
        if (isEmpty(content)) {
            return post;
        }
        try {
            post.setEntity(new StringEntity(content));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
        }
        return post;
    }

}
