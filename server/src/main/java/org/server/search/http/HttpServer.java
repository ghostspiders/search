/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.server.search.http;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.settings.Settings;

import java.io.IOException;

import static org.server.search.http.HttpResponse.Status.*;

// 定义一个HTTP服务器类，继承自AbstractComponent并实现LifecycleComponent接口
public class HttpServer extends AbstractComponent implements LifecycleComponent<HttpServer> {

    // 生命周期管理对象
    private final Lifecycle lifecycle = new Lifecycle();

    // HTTP服务器传输层
    private final HttpServerTransport transport;

    // 线程池，用于处理并发请求
    private final ThreadPool threadPool;

    // 用于存储不同HTTP方法的处理器的路径映射
    private final PathTrie<HttpServerHandler> getHandlers;
    private final PathTrie<HttpServerHandler> postHandlers;
    private final PathTrie<HttpServerHandler> putHandlers;
    private final PathTrie<HttpServerHandler> deleteHandlers;

    // 构造函数，通过依赖注入初始化HTTP服务器
    @Inject public HttpServer(Settings settings, HttpServerTransport transport, ThreadPool threadPool) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;

        // 初始化各种HTTP方法的处理器映射
        getHandlers = new PathTrie<HttpServerHandler>();
        postHandlers = new PathTrie<HttpServerHandler>();
        putHandlers = new PathTrie<HttpServerHandler>();
        deleteHandlers = new PathTrie<HttpServerHandler>();

        // 设置传输层的请求分发适配器
        transport.httpServerAdapter(new HttpServerAdapter() {
            @Override public void dispatchRequest(HttpRequest request, HttpChannel channel) {
                internalDispatchRequest(request, channel); // 内部分发请求
            }
        });
    }

    // 获取当前生命周期状态
    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    // 注册请求处理器
    public void registerHandler(HttpRequest.Method method, String path, HttpServerHandler handler) {
        // 根据请求方法，将处理器注册到对应的路径映射中
        if (method == HttpRequest.Method.GET) {
            getHandlers.insert(path, handler);
        } else if (method == HttpRequest.Method.POST) {
            postHandlers.insert(path, handler);
        } else if (method == HttpRequest.Method.PUT) {
            putHandlers.insert(path, handler);
        } else if (method == HttpRequest.Method.DELETE) {
            deleteHandlers.insert(path, handler);
        }
    }

    // 启动服务器
    public HttpServer start() throws Exception {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        transport.start(); // 启动传输层
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress()); // 日志记录绑定地址
        }
        return this;
    }

    // 停止服务器
    public HttpServer stop() throws SearchException, InterruptedException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        transport.stop(); // 停止传输层
        return this;
    }

    // 关闭服务器
    public void close() throws InterruptedException {
        if (lifecycle.started()) {
            stop(); // 如果服务器已启动，则先停止
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        transport.close(); // 关闭传输层·
    }

    // 内部请求分发逻辑
    private void internalDispatchRequest(final HttpRequest request, final HttpChannel channel) {
        // 获取请求处理器
        final HttpServerHandler httpHandler = getHandler(request);
        if (httpHandler != null) {
            // 如果处理器需要在新线程中执行，则使用线程池执行
            if (httpHandler.spawn()) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            httpHandler.handleRequest(request, channel); // 处理请求
                        } catch (Exception e) {
                            // 发送异常响应
                            try {
                                channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                            } catch (IOException e1) {
                                logger.error("Failed to send failure response for uri [" + request.uri() + "]", e1);
                            }
                        }
                    }
                });
            } else {
                // 直接在当前线程处理请求
                try {
                    httpHandler.handleRequest(request, channel);
                } catch (Exception e) {
                    try {
                        channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                    } catch (IOException e1) {
                        logger.error("Failed to send failure response for uri [" + request.uri() + "]", e1);
                    }
                }
            }
        } else {
            // 如果没有找到处理器，发送错误响应
            channel.sendResponse(new StringHttpResponse(BAD_REQUEST, "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
        }
    }

    // 根据请求获取处理器
    private HttpServerHandler getHandler(HttpRequest request) {
        // 获取请求路径
        String path = getPath(request);
        HttpRequest.Method method = request.method();
        // 根据请求方法和路径获取处理器
        if (method == HttpRequest.Method.GET) {
            return getHandlers.retrieve(path, request.params());
        } else if (method == HttpRequest.Method.POST) {
            return postHandlers.retrieve(path, request.params());
        } else if (method == HttpRequest.Method.PUT) {
            return putHandlers.retrieve(path, request.params());
        } else if (method == HttpRequest.Method.DELETE) {
            return deleteHandlers.retrieve(path, request.params());
        } else {
            return null;
        }
    }

    // 从请求URI中提取路径
    private String getPath(HttpRequest request) {
        String uri = request.uri();
        int questionMarkIndex = uri.indexOf('?');
        if (questionMarkIndex == -1) {
            return uri;
        }
        return uri.substring(0, questionMarkIndex);
    }
}