package org.example.http;

import java.util.List;
import java.util.Map;

/**
 * @author gaoyvfeng
 * @ClassName Netty4HttpRequest
 * @description:
 * @datetime 2024年 02月 01日 17:58
 * @version: 1.0
 */
public class Netty4HttpRequest implements HttpRequest{
    @Override
    public Method method() {
        return null;
    }

    @Override
    public String uri() {
        return null;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return null;
    }

    @Override
    public BytesReference content() {
        return null;
    }

    @Override
    public List<String> getCookies() {
        return null;
    }

    @Override
    public HttpVersion protocolVersion() {
        return null;
    }

    @Override
    public HttpRequest removeHeader(String header) {
        return null;
    }

    @Override
    public HttpResponse createResponse(RestStatus status, BytesReference content) {
        return null;
    }

    @Override
    public void release() {

    }
}
