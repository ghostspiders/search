package org.example.tcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.util.Map;

public class Netty4Transport {

    private volatile ServerBootstrap serverBootstrap;
    private volatile Bootstrap clientBootstrap;
    private  Map<String, String> settings;

    public Netty4Transport(Map<String, String> settings) {
        this.settings = settings;
    }
    protected void doStart() {
        boolean success = false;
        try {
            clientBootstrap = createClientBootstrap();
            serverBootstrap = createServerBootstrap();
            bindServer();
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
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

    private void bindServer() {
        try {
            ChannelFuture serverFuture = serverBootstrap.bind(Integer.parseInt(this.settings.get("transport.tcp.port"))).sync();
            serverFuture.channel().closeFuture().sync();

            ChannelFuture clientFuture = clientBootstrap.bind(Integer.parseInt(this.settings.get("transport.tcp.port"))).sync();
            clientFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ServerBootstrap createServerBootstrap(){
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new StringDecoder(CharsetUtil.UTF_8));
                        p.addLast(new StringEncoder(CharsetUtil.UTF_8));
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        return serverBootstrap;
    }

    private Bootstrap createClientBootstrap() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap clientBootstrap = new ServerBootstrap();
            clientBootstrap.group(bossGroup, workerGroup)
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
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
        return clientBootstrap;
    }


    protected class ClientChannelInitializer extends ChannelInitializer<Channel> {
        @Override
        protected void initChannel(Channel ch) throws Exception {

        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    protected class ServerChannelInitializer extends ChannelInitializer<Channel> {
        protected final String name;

        protected ServerChannelInitializer(String name) {
            this.name = name;
        }
        @Override
        protected void initChannel(Channel ch) throws Exception {

        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }
    @ChannelHandler.Sharable
    private static class ServerChannelExceptionHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }
    private void setupPipeline(Channel ch, boolean isServerChannel) {
        final var pipeline = ch.pipeline();
        pipeline.addLast("", null);
        pipeline.addLast("", null);
    }
}
