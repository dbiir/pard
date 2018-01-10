package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
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

import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardExchangeServer
        implements Runnable
{
    private final Logger logger = Logger.getLogger(PardExchangeServer.class.getName());
    private final int port;
    private final PardTaskExecutor executor;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public PardExchangeServer(int port, PardTaskExecutor executor)
    {
        this.port = port;
        this.executor = executor;
    }

    /**
     * pipeline inbound: ByteToTaskDecoder -> TaskHandler
     * pipeline outbound: ResultSetToByteEncoder
     * */
    @Override
    public void run()
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
                                            new ObjectDecoder(100 * 1024 * 1024, ClassResolvers.cacheDisabled(null)),
                                            new ExchangeTaskHandler(executor));
                        }
                    });
            ChannelFuture f = serverBootstrap.bind(port).sync();
            logger.info("Exchange server started at port: " + port);
            f.channel().closeFuture().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            stop();
        }
    }

    public void stop()
    {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
}
