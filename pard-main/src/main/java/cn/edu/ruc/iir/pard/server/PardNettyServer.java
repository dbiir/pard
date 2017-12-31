package cn.edu.ruc.iir.pard.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
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
public class PardNettyServer
{
    private final int port;
    private final List<ChannelHandlerAdapter> adapters;

    public PardNettyServer(int port)
    {
        this.port = port;
        this.adapters = new ArrayList<>();
    }

    public void addAdapter(ChannelHandlerAdapter adapter)
    {
        this.adapters.add(adapter);
    }

    public void run()
    {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
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
                                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                            for (ChannelHandlerAdapter adapter : adapters) {
                                ch.pipeline().addLast(adapter);
                            }
                        }
                    });

            serverBootstrap.bind(port).channel().closeFuture().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
