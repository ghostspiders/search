

package org.example.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import org.example.action.ActionListener;
import org.example.exception.TransportException;

import java.net.InetSocketAddress;

public class Netty4TcpChannelTest {

    private final Channel channel;
    private final boolean isServer;
    private final ActionListener connectContext;
    private final ActionListener closeContext = new ActionListener();

    Netty4TcpChannelTest(Channel channel, boolean isServer, ChannelFuture connectFuture) {
        this.channel = channel;
        this.isServer = isServer;
        this.connectContext = new ActionListener();
        addListener(this.channel.closeFuture(), closeContext);
        addListener(connectFuture, connectContext);
    }

    public static void addListener(ChannelFuture channelFuture, ActionListener listener) {
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(null);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    listener.onFailure(new Exception(cause));
                } else {
                    listener.onFailure((Exception) cause);
                }
            }
        });
    }

    public static ChannelPromise addPromise(ActionListener listener, Channel channel) {
        ChannelPromise writePromise = channel.newPromise();
        writePromise.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(null);
            } else {
                final Throwable cause = f.cause();
                if (cause instanceof Error) {
                    listener.onFailure(new Exception(cause));
                } else {
                    listener.onFailure((Exception) cause);
                }
            }
        });
        return writePromise;
    }

    public void close() {
            channel.close();
    }


    public boolean isServerChannel() {
        return isServer;
    }



    public void addCloseListener(ActionListener listener) {
        closeContext.addListener(listener);
    }

    public void addConnectListener(ActionListener listener) {
        connectContext.addListener(listener);
    }

    public boolean isOpen() {
        return channel.isOpen();
    }

    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    public void sendMessage(ByteBuf byteBuf, ActionListener listener) {
        channel.writeAndFlush(byteBuf, addPromise(listener, channel));
        if (channel.eventLoop().isShutdown()) {
            listener.onFailure(new TransportException("Cannot send message, channel is shutting down."));
        }
    }

    public Channel getNettyChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "Netty4TcpChannel{"
            + "localAddress="
            + getLocalAddress()
            + ", remoteAddress="
            + channel.remoteAddress()
            + '}';
    }
}
