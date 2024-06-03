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

package org.server.search.http.netty;

import com.google.inject.Inject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.server.search.SearchException;
import org.server.search.http.*;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.BindTransportException;
import org.server.search.util.SizeValue;
import org.server.search.util.TimeValue;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.settings.Settings;
import org.server.search.util.transport.BoundTransportAddress;
import org.server.search.util.transport.InetSocketTransportAddress;
import org.server.search.util.transport.NetworkExceptionHelper;
import org.server.search.util.transport.PortsRange;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.server.search.util.TimeValue.*;
import static org.server.search.util.concurrent.DynamicExecutors.*;
import static org.server.search.util.io.HostResolver.*;

 
public class NettyHttpServerTransport extends AbstractComponent implements HttpServerTransport {

    // 生命周期管理对象，用于管理HTTP服务器的启动、停止等状态
    private final Lifecycle lifecycle = new Lifecycle();

    // 线程池，用于执行HTTP服务器相关的任务
    private final ThreadPool threadPool;

    // 工作线程数量
    private final int workerCount;

    // 用于绑定HTTP服务器的端口号
    private final String port;

    // 用于绑定HTTP服务器的主机地址
    private final String bindHost;

    // 用于发布（即外部访问）的主机地址
    private final String publishHost;

    // 是否启用TCP_NODELAY选项，用于禁用Nagle's算法
    private final Boolean tcpNoDelay;

    // 是否启用TCP_KEEPALIVE选项，用于保持TCP连接活跃
    private final Boolean tcpKeepAlive;

    // 是否重用地址
    private final Boolean reuseAddress;

    // TCP发送缓冲区大小
    private final SizeValue tcpSendBufferSize;

    // TCP接收缓冲区大小
    private final SizeValue tcpReceiveBufferSize;

    // HTTP连接的keep-alive超时时间
    private final TimeValue httpKeepAlive;

    // HTTP keep-alive的心跳检测间隔
    private final TimeValue httpKeepAliveTickDuration;

    // Netty的服务器启动工具，用于初始化和启动服务器
    private volatile ServerBootstrap serverBootstrap;

    // 已绑定的传输地址，存储服务器绑定的地址和端口信息
    private volatile BoundTransportAddress boundAddress;

    // Netty的ChannelFuture，表示服务器Channel的异步操作结果
    private volatile ChannelFuture serverChannelFuture;

    // HTTP服务器适配器，用于将Netty的事件分派到Elasticsearch的HTTP请求处理逻辑
    private volatile HttpServerAdapter httpServerAdapter;

    @Inject public NettyHttpServerTransport(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;
        this.workerCount = componentSettings.getAsInt("workerCount", Runtime.getRuntime().availableProcessors());
        this.port = componentSettings.get("port", "9200-9300");
        this.bindHost = componentSettings.get("bindHost");
        this.publishHost = componentSettings.get("publishHost");
        this.tcpNoDelay = componentSettings.getAsBoolean("tcpNoDelay", true);
        this.tcpKeepAlive = componentSettings.getAsBoolean("tcpKeepAlive", null);
        this.reuseAddress = componentSettings.getAsBoolean("reuseAddress", true);
        this.tcpSendBufferSize = componentSettings.getAsSize("tcpSendBufferSize", null);
        this.tcpReceiveBufferSize = componentSettings.getAsSize("tcpReceiveBufferSize", null);
        this.httpKeepAlive = componentSettings.getAsTime("httpKeepAlive", timeValueSeconds(30));
        this.httpKeepAliveTickDuration = componentSettings.getAsTime("httpKeepAliveTickDuration", timeValueMillis(500));

        if ((httpKeepAliveTickDuration.millis() * 10) > httpKeepAlive.millis()) {
            logger.warn("Suspicious keep alive settings, httpKeepAlive set to [{}], while httpKeepAliveTickDuration is set to [{}]", httpKeepAlive, httpKeepAliveTickDuration);
        }
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    public void httpServerAdapter(HttpServerAdapter httpServerAdapter) {
        this.httpServerAdapter = httpServerAdapter;
    }

    // HttpServerTransport的start方法，用于启动HTTP服务器传输层
    @Override
    public HttpServerTransport start() throws HttpException {
        if (!lifecycle.moveToStarted()) {
            return this; // 如果无法转换到启动状态，则直接返回当前实例
        }

        // 创建Boss线程组，用于接受连接，这里使用单个线程
        EventLoopGroup bossGroup = new NioEventLoopGroup(1, Executors.newCachedThreadPool(daemonThreadFactory(settings, "httpBoss")));
        // 创建Worker线程组，用于处理接受到的连接
        EventLoopGroup workerGroup = new NioEventLoopGroup(4, Executors.newCachedThreadPool(daemonThreadFactory(settings, "httpIoWorker")));

        // 初始化ServerBootstrap，用于启动Netty服务器
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup); // 设置线程组
        serverBootstrap.channel(NioServerSocketChannel.class); // 设置服务器通道类型为NioServerSocketChannel

        // 创建HTTP Keep-Alive定时器
        HashedWheelTimer keepAliveTimer = new HashedWheelTimer(daemonThreadFactory(settings, "keepAliveTimer"),
                httpKeepAliveTickDuration.millis(), TimeUnit.MILLISECONDS);

        // 创建HttpRequestHandler处理器
        HttpRequestHandler requestHandler = new HttpRequestHandler(this);

        // 设置ServerBootstrap的子通道初始化器
        serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                // 添加Netty的HTTP请求解码器
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                // 添加HTTP对象聚合器，用于将多个小块数据合并为一个完整的HTTP请求
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65535));
                // 添加HTTP响应编码器
                ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                // 添加分块写处理器
                ch.pipeline().addLast("chunked", new ChunkedWriteHandler());
                // 添加自定义的HttpRequestHandler处理器
                ch.pipeline().addLast("handler", requestHandler);
            }
        });

        // 设置TCP参数
        if (tcpNoDelay != null) {
            serverBootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
        }
        if (tcpKeepAlive != null) {
            serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive);
        }
        if (tcpSendBufferSize != null) {
            serverBootstrap.childOption(ChannelOption.SO_SNDBUF, (int) tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null) {
            serverBootstrap.childOption(ChannelOption.SO_RCVBUF, (int) tcpReceiveBufferSize.bytes());
        }
        if (reuseAddress != null) {
            serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
            serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, reuseAddress);
        }

        // 绑定并启动服务器以接受传入连接
        InetAddress hostAddressX;
        try {
            hostAddressX = resultBindHostAddress(bindHost, settings); // 解析绑定主机地址
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress hostAddress = hostAddressX;

        // 端口范围
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                try {
                    // 绑定到指定端口并启动服务器
                    serverChannelFuture = serverBootstrap.bind(new InetSocketAddress(hostAddress, portNumber)).sync();
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            }
        });
        if (!success) {
            throw new BindHttpException("Failed to bind to [" + port + "]", lastException.get());
        }

        // 获取绑定的地址
        InetSocketAddress boundAddress = (InetSocketAddress) serverChannelFuture.channel().localAddress();
        InetSocketAddress publishAddress;
        try {
            InetAddress publishAddressX = resultPublishHostAddress(publishHost, settings);
            if (publishAddressX == null) {
                // 如果发布地址为null，则使用绑定地址
                publishAddress = boundAddress;
            } else {
                publishAddress = new InetSocketAddress(publishAddressX, boundAddress.getPort());
            }
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
        return this; // 返回当前HttpServerTransport实例
    }

    @Override public HttpServerTransport stop() throws SearchException, InterruptedException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        if (serverChannelFuture != null) {
            serverChannelFuture.channel().close().awaitUninterruptibly();
            serverChannelFuture = null;
        }
        if (serverBootstrap != null) {
            serverBootstrap.config().group().shutdownGracefully().sync();
            serverBootstrap = null;
        }
        return this;
    }

    @Override public void close() throws InterruptedException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    void dispatchRequest(HttpRequest request, HttpChannel channel) {
        httpServerAdapter.dispatchRequest(request, channel);
    }

    void exceptionCaught(ChannelHandlerContext ctx, Throwable e){
        if (e instanceof ReadTimeoutException) {
            if (logger.isTraceEnabled()) {
                logger.trace("Connection timeout [{}]", ctx.channel().remoteAddress());
            }
            ctx.channel().close();
        } else {
            if (!lifecycle.started()) {
                // ignore
                return;
            }
            if (!NetworkExceptionHelper.isCloseConnectionException(e.getCause())) {
                logger.warn("Caught exception while handling client http trafic", e.getCause());
            }
        }
    }
}
