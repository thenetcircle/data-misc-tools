package com.thenetcircle.service.data.access.http;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SimpleHttpReq implements Serializable {
    public final String urlStr;
    public final Map<String, String> headers;
    public final String content;
    public final String method;
    private int hashCode;

    public SimpleHttpReq(String method, String urlStr, Map<String, String> headers, String content) {
        this.method = method;
        this.urlStr = urlStr;
        this.headers = headers;
        this.content = content;

        hashCode = Objects.hash(method, urlStr, content, headers);
    }

    public SimpleHttpReq() {
        this("GET", "", new HashMap<>(), "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleHttpReq)) return false;
        SimpleHttpReq that = (SimpleHttpReq) o;
        return Objects.equals(urlStr, that.urlStr) &&
            Objects.equals(headers, that.headers) &&
            Objects.equals(content, that.content) &&
            Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
