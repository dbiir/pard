package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;

/**
 * pard
 *
 * @author guodong
 */
public class PardExchangeClient
{
    private final String host;
    private final int port;
    private EventLoopGroup group;

    public PardExchangeClient(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public void connect(Task task, BlockingQueue<Block> blocks)
    {
        this.group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        public void initChannel(SocketChannel ch)
                        {
                            ch.pipeline()
                                    .addLast(new ObjectEncoder(),
                                            new ObjectDecoder(100 * 1024 * 1024, ClassResolvers.cacheDisabled(null)),
                                            new ExchangeBlockHandler(task, blocks));
                        }});
            ChannelFuture f = bootstrap.connect(new InetSocketAddress(host, port)).sync();
            Channel channel = f.channel();
            ChannelFuture taskFuture = channel.writeAndFlush(task);
            taskFuture.addListener((ChannelFutureListener) future -> System.out.println("Task write complete"));
            channel.closeFuture().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            close();
        }
    }

    public void close()
    {
        group.shutdownGracefully();
    }
}
