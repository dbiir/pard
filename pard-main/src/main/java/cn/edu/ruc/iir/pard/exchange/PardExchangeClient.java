package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.commons.memory.Block;
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
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * pard
 *
 * @author guodong
 */
public class PardExchangeClient
{
    private final String host;
    private final int port;
    private final ConcurrentLinkedQueue<Block> blocks;
    private EventLoopGroup group;

    public PardExchangeClient(String host, int port)
    {
        this.host = host;
        this.port = port;
        this.blocks = new ConcurrentLinkedQueue<>();
    }

    public Block connect(Task task)
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
                                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                            new ExchangeBlockHandler(blocks));
                        }});
            ChannelFuture f = bootstrap.connect(new InetSocketAddress(host, port)).sync();
            Channel channel = f.channel();
            ChannelFuture taskFuture = channel.writeAndFlush(task);
            taskFuture.addListener((ChannelFutureListener) future -> System.out.println("Task write complete"));
            while (blocks.size() == 0) {
                    // do nothing but wait
            }
            Block block = blocks.poll();
            System.out.println(block);
            return block;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            close();
        }
        return null;
    }

    public void close()
    {
        group.shutdownGracefully();
    }
}
