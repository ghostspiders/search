package org.example.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.example.action.ActionListener;
import org.example.tcp.Netty4TcpChannel;

import java.net.InetSocketAddress;

/**
 * @author gaoyvfeng
 * @ClassName Netty4HttpServerChannel
 * @description:
 * @datetime 2024年 02月 04日 17:41
 * @version: 1.0
 */
public class Netty4HttpServerChannel {
    private final Channel channel;
    private final ActionListener closeContext = new ActionListener();

    Netty4HttpServerChannel(Channel channel) {
        this.channel = channel;
        Netty4TcpChannel.addListener(this.channel.closeFuture(), closeContext);
    }

    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    public void addCloseListener(ActionListener listener) {
        closeContext.addListener(listener);
    }

    public boolean isOpen() {
        return channel.isOpen();
    }

    public void close() {
        channel.close();
    }

    public String toString() {
        return "Netty4HttpChannel{localAddress=" + getLocalAddress() + "}";
    }

}
