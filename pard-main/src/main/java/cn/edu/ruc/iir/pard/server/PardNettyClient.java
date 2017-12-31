package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.executor.connector.Task;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class PardNettyClient
{
    private final String host;
    private final int port;
    private final List<ChannelHandlerAdapter> adapters;
    private Task task;

    public PardNettyClient(String host, int port)
    {
        this.host = host;
        this.port = port;
        this.adapters = new ArrayList<>();
    }

    public void addAdapter(ChannelHandlerAdapter adapter)
    {
        this.adapters.add(adapter);
    }

    public void setTask(Task task)
    {
        this.task = task;
    }

    public void run()
    {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                        {
                            ch.pipeline()
                                    .addLast(new ObjectEncoder(),
                                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                            for (ChannelHandlerAdapter adapter : adapters) {
                                ch.pipeline().addLast(adapter);
                            }
                        }
                    });

            Channel channel = bootstrap.connect(this.host, this.port).sync().channel();
            ChannelFuture future = channel.writeAndFlush(task);
            channel.closeFuture().sync();

            if (future != null) {
                future.sync();
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            group.shutdownGracefully();
        }
    }
}
