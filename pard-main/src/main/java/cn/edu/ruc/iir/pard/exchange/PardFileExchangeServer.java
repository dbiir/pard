package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;

import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */

public class PardFileExchangeServer
        implements Runnable
{
    private final Logger logger = Logger.getLogger(PardFileExchangeServer.class.getName());
    private final int port;
    private final PardTaskExecutor executor;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public PardFileExchangeServer(int port, PardTaskExecutor executor)
    {
        this.port = port;
        this.executor = executor;
    }

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
                                    .addLast(
                                            new StringEncoder(CharsetUtil.UTF_8),
                                            new LineBasedFrameDecoder(8192),
                                            new StringDecoder(CharsetUtil.UTF_8),
                                            new ChunkedWriteHandler(),
                                            new ExchangeFileReceiveHandler(executor));
                        }
                    });
            ChannelFuture f = serverBootstrap.bind(port).sync();
            logger.info("Exchange file server started at port: " + port);
            f.channel().closeFuture().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void stop()
    {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
}
