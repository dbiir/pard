package cn.edu.ruc.iir.pard.exchange;

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

/**
 * pard
 *
 * @author guodong
 */
public class PardFileExchangeServer
{
    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public PardFileExchangeServer(int port)
    {
        this.port = port;
    }

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
                                            new ExchangeFileReceiveHandler());
                        }
                    });
            ChannelFuture f = serverBootstrap.bind(port).sync();
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
}
