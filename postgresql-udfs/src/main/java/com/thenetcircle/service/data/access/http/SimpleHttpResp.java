package com.thenetcircle.service.data.access.http;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SimpleHttpResp implements Serializable {
    public final int statusCode;
    public final String content;
    public final Map<String, String> headers;

    private int hashCode;

    public SimpleHttpResp() {
        this(-1, "", new HashMap<>());
    }

    public SimpleHttpResp(int statusCode, String content, Map<String, String> headers) {
        this.statusCode = statusCode;
        this.content = content;
        this.headers = headers;
        hashCode = Objects.hash(statusCode, content, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleHttpResp)) return false;
        SimpleHttpResp that = (SimpleHttpResp) o;
        return statusCode == that.statusCode &&
            Objects.equals(content, that.content) &&
            Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
