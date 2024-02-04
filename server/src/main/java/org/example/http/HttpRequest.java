package org.example.http;

import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Map;

/**
 * @author gaoyvfeng
 * @ClassName HttpRequest
 * @description:
 * @datetime 2024年 02月 01日 17:07
 * @version: 1.0
 */
public interface HttpRequest {
    enum HttpVersion {
        HTTP_1_0,
        HTTP_1_1
    }
    public enum Method {
        GET,
        POST,
        PUT,
        DELETE,
        OPTIONS,
        HEAD,
        PATCH,
        TRACE,
        CONNECT
    }
    Method method();

    String uri();

    Map<String, List<String>> getHeaders();

    ByteBuf content();

    List<String> getCookies();

    HttpVersion protocolVersion();

    HttpRequest removeHeader(String header);

    HttpResponse createResponse(RestStatus status, ByteBuf content);

    void release();

    default String header(String name) {
        List<String> values = getHeaders().get(name);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }
}
