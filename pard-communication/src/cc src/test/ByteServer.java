package test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import transfer.TableSchema;

/**
 * Discards any incoming data.
 * 可以轻易改造成 echo server, time server
 */
public class ByteServer {

    private int port;
    private byte[] data;
    ByteServerHandler byteServerHandler;

    public ByteServer(int port, byte[] data) {
        this.port = port;
        this.data =data;
        byteServerHandler =new ByteServerHandler();
    }


    public void setData(byte[] data){
        byteServerHandler.setData(data);
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
            f.channel().closeFuture().sync();
        } finally {
            System.out.println("hahaha");
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();

        }
    }

//    10.48.169.27
    public static void main(String[] args) throws Exception {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 8081;
        }

        TableSchema ts= TestObj.tableSchema1();
        System.out.println("server  ts:");
        System.out.println(ts);
        byte[] data = transfer.SerializationUtils.serialize(ts);
        ByteServer bs=new ByteServer(port, data);
        bs.setData(data);
        bs.run();

    }
}