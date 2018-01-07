package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.TestTask;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * pard
 *
 * @author guodong
 */
public class PardNettyExchangeClient
{
    private final String host;
    private final int port;
    private final ConcurrentLinkedQueue<PardResultSet> resultSets;
    private final List<PardResultSet> localRS = new ArrayList<>();
    private EventLoopGroup group;

    public PardNettyExchangeClient(String host, int port)
    {
        this.host = host;
        this.port = port;
        this.resultSets = new ConcurrentLinkedQueue<>();
    }

    public void connect()
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
                                            new ExchangeResultSetHandler(resultSets));
                        }});
            ChannelFuture f = bootstrap.connect(new InetSocketAddress(host, port)).sync();
            Channel channel = f.channel();
            ChannelFuture taskFuture = channel.writeAndFlush(new TestTask("site0"));
            taskFuture.addListener((ChannelFutureListener) future -> System.out.println("Task write complete"));
            for (;; ) {
                while (resultSets.size() == 0) {
                    // do nothing but wait
                }
                PardResultSet r = resultSets.poll();
                System.out.println(r);
                if (r.getStatus() == PardResultSet.ResultStatus.EOR) {
                    System.out.println("End of client session");
                    break;
                }
                localRS.add(r);
//                channel.writeAndFlush(new NextRSTask("site0"));
            }
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

    public static void main(String[] args)
    {
        PardNettyExchangeClient exchangeClient = new PardNettyExchangeClient("127.0.0.1", 10012);
        exchangeClient.connect();
        for (PardResultSet r : exchangeClient.localRS) {
            System.out.println("RE: " + r);
        }
    }
}
