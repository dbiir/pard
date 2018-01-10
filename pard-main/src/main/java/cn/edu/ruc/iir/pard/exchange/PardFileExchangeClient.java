package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * pard
 *
 * @author guodong
 */
public class PardFileExchangeClient
{
    private final String host;
    private final int port;
    private final String path;
    private final String schema;
    private final String table;
    private final String taskId;
    private final ConcurrentLinkedQueue<PardResultSet> resultSets;
    private EventLoopGroup group;

    public PardFileExchangeClient(String host, int port, String path,
                                  String schema, String table, String taskId,
                                  ConcurrentLinkedQueue<PardResultSet> resultSets)
    {
        this.host = host;
        this.port = port;
        this.path = path;
        this.schema = schema;
        this.table = table;
        this.taskId = taskId;
        this.resultSets = resultSets;
    }

    public void run()
    {
        this.group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>()
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
                                            new ExchangeFileSendHandler(path, schema, table, taskId, resultSets));
                        }});
            ChannelFuture f = bootstrap.connect(new InetSocketAddress(host, port)).sync();
            f.channel().closeFuture().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            group.shutdownGracefully();
        }
    }
}
