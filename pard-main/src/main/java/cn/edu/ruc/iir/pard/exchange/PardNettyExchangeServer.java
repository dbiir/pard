package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.Connector;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * pard
 *
 * @author guodong
 */
public class PardNettyExchangeServer
{
    private final int port;
    private final Connector connector;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public PardNettyExchangeServer(int port, Connector connector)
    {
        this.port = port;
        this.connector = connector;
    }

    /**
     * pipeline inbound: ByteToTaskDecoder -> TaskHandler
     * pipeline outbound: ResultSetToByteEncoder
     * */
    public ChannelFuture start()
    {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        public void initChannel(SocketChannel ch)
                        {
                            ch.pipeline()
                                    .addLast(new ObjectEncoder(),
                                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                            new ExchangeTaskHandler(null));
                        }
                    });
            ChannelFuture f = serverBootstrap.bind(port).sync();
            f.channel().closeFuture().sync();
            return f;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            stop();
        }
        return null;
    }

    public void stop()
    {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    public static void main(String[] args)
    {
        PardNettyExchangeServer exchangeServer = new PardNettyExchangeServer(10012, null);
        exchangeServer.start();
    }
}
