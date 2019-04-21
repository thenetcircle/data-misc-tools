package com.thenetcircle.service.data.access.http;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
            .filter(en -> !Objects.isNull(en.getKey()))
            .filter(en -> StringUtils.isNotBlank(en.getKey().toString()))
            .map(en -> new BasicHeader(String.valueOf(en.getKey()), String.valueOf(en.getValue())))
            .toArray(Header[]::new);
    }

    static void close(CloseableHttpClient hc) throws IOException {
        if (hc == null) return;
        try {
            hc.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static String urlEncode(String urlStr) {
        try {
            return URLEncoder.encode(urlStr, UTF_8);
        } catch (UnsupportedEncodingException e) {
            return e.getLocalizedMessage();
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
        try {
            post.setEntity(new StringEntity(content));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return post;
    }

}
