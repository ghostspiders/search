package org.example.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class Netty4HttpServerTransport {
    private static final Logger logger = LogManager.getLogger(Netty4HttpServerTransport.class);
    private volatile ServerBootstrap serverBootstrap;
    private volatile Bootstrap clientBootstrap;
    private  Map<String, String> settings;
    private  Dispatcher dispatcher;
    public Netty4HttpServerTransport(Map<String, String> settings, Dispatcher dispatcher) {
        this.settings = settings;
        this.dispatcher = dispatcher;

    }
    protected void doStart() {
        boolean success = false;
        try {
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ChannelPipeline p = ch.pipeline();
                                p.addLast(new HttpServerCodec());
                                p.addLast(new HttpObjectAggregator(512 * 1024));
                            }
                        })
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true);
                bindServer();
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }
    private void bindServer() {
        try {
            ChannelFuture serverFuture = serverBootstrap.bind(Integer.parseInt(this.settings.get("transport.port"))).sync();
            serverFuture.channel().closeFuture().sync();

            ChannelFuture clientFuture = clientBootstrap.bind(Integer.parseInt(this.settings.get("transport.port"))).sync();
            clientFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    private void doStop() {
        try {
            Channel channel = clientBootstrap.connect().sync().channel();
            channel.close().sync();
            EventLoopGroup workerGroup = clientBootstrap.config().group();
            workerGroup.shutdownGracefully().sync();

            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            Channel serverChannel = channelFuture.channel();
            serverChannel.close().sync();
            EventLoopGroup bossGroup = serverBootstrap.config().group();
            bossGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ChannelHandler.Sharable
    private static class ServerChannelExceptionHandler extends ChannelInboundHandlerAdapter {
        private ServerChannelExceptionHandler() {}
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        }
    }
}
