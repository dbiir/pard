package cn.edu.ruc.iir.pard.communication.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 调用 setData() 改变发送的数据
 */
public class ByteServer {

    private int port;
    private byte[] data;

    public ByteServer(int port) {
        this.port = port;
    }


    public void setData(byte[] data){
        this.data =data;
    }

    public void run() throws Exception {

        EventLoopGroup bossGroup = new NioEventLoopGroup(); //  is a multithreaded event loop that handles I/O operation.
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap(); // is a helper class that sets up a server.
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // we specify to use the NioServerSocketChannel class which is used to instantiate a new Channel to accept incoming connections.
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
//                            ch.pipeline().addLast(new DiscardServerHandler()); //for ByteServer
//                            ch.pipeline().addLast(new TimeServerHandler());

                            //每次都新建一个server handler，不然会第二次调用会报错not sharable.  或者能找到关闭服务器的方法也好。
                            ByteServerHandler byteServerHandler = new ByteServerHandler();
                            byteServerHandler.setData(data);
                            System.out.println("每次新来一个client 会走到这里"); //

                            ch.pipeline().addLast(byteServerHandler);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)          // (5)
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync(); // (7)

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            System.out.println("61");
            f.channel().closeFuture().sync();//全卡在这了！
            System.out.println("hahaha");
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();

        }
    }

//    10.48.169.27
    public static void main(String[] args) throws Exception {

    }
}