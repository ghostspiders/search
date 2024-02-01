package org.example.http;

/**
 * @author gaoyvfeng
 * @ClassName HttpResponse
 * @description:
 * @datetime 2024年 02月 01日 17:10
 * @version: 1.0
 */
public interface HttpResponse {
    void addHeader(String name, String value);

    boolean containsHeader(String name);
}
