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

    private final Lifecycle lifecycle = new Lifecycle();

    private final ThreadPool threadPool;

    private final int workerCount;

    private final String port;

    private final String bindHost;

    private final String publishHost;

    private final Boolean tcpNoDelay;

    private final Boolean tcpKeepAlive;

    private final Boolean reuseAddress;

    private final SizeValue tcpSendBufferSize;

    private final SizeValue tcpReceiveBufferSize;

    private final TimeValue httpKeepAlive;

    private final TimeValue httpKeepAliveTickDuration;

    private volatile ServerBootstrap serverBootstrap;

    private volatile BoundTransportAddress boundAddress;

    private volatile ChannelFuture  serverChannelFuture;

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

    @Override public HttpServerTransport start() throws HttpException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        EventLoopGroup bossGroup = new NioEventLoopGroup(1, Executors.newCachedThreadPool(daemonThreadFactory(settings, "httpBoss")));
        EventLoopGroup workerGroup = new NioEventLoopGroup(4, Executors.newCachedThreadPool(daemonThreadFactory(settings, "httpIoWorker")));
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup,workerGroup);
        serverBootstrap.channel(NioServerSocketChannel.class);
        HashedWheelTimer keepAliveTimer = new HashedWheelTimer(daemonThreadFactory(settings, "keepAliveTimer"), httpKeepAliveTickDuration.millis(), TimeUnit.MILLISECONDS);
        HttpRequestHandler requestHandler = new HttpRequestHandler(this);

        serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65535));
                ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                ch.pipeline().addLast("chunked", new ChunkedWriteHandler());
                ch.pipeline().addLast("handler", requestHandler);
            }
        });
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

        // Bind and start to accept incoming connections.
        InetAddress hostAddressX;
        try {
            hostAddressX = resultBindHostAddress(bindHost, settings);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress hostAddress = hostAddressX;

        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override public boolean onPortNumber(int portNumber) {
                try {
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

        InetSocketAddress boundAddress = (InetSocketAddress) serverChannelFuture.channel().localAddress();
        InetSocketAddress publishAddress;
        try {
            InetAddress publishAddressX = resultPublishHostAddress(publishHost, settings);
            if (publishAddressX == null) {
                // if its 0.0.0.0, we can't publish that.., default to the local ip address 
                if (boundAddress.getAddress().isAnyLocalAddress()) {
                    publishAddress = new InetSocketAddress(resultPublishHostAddress(publishHost, settings, LOCAL_IP), boundAddress.getPort());
                } else {
                    publishAddress = boundAddress;
                }
            } else {
                publishAddress = new InetSocketAddress(publishAddressX, boundAddress.getPort());
            }
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
        return this;
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
