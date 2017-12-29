package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.communication.rpc.PardRPCService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * pard
 *
 * @author guodong
 */
public class PardServer
{
    private final PardUserConfiguration configuration;

    private Server server;       // rpc server
    private ServerListener serverListener;

    PardServer(String configurationPath)
    {
        this.configuration = PardUserConfiguration.INSTANCE();
        configuration.init(configurationPath);
    }

    private void startup()
    {
        PardStartupPipeline pipeline = new PardStartupPipeline();

        pipeline.addStartupHook(
                () -> {
                    int port = configuration.getServerPort();
                    ServerBuilder<NettyServerBuilder> serverBuilder =
                            NettyServerBuilder.forPort(port);
                    serverBuilder.addService(new PardRPCService());
                    server = serverBuilder.build();
//                        server.start();
                    new Thread(() -> {
                        try {
                            server.start();
                            System.out.println("RPC Server Started");
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }).start();
                });

        pipeline.addStartupHook(
                () -> Runtime.getRuntime().addShutdownHook(
                        new Thread(PardServer.this::stop)));

        // start socket listener
        pipeline.addStartupHook(this::startListener);

        // add server running in loop hook
        pipeline.addStartupHook(
                this::blockUntilTermination);

        try {
            pipeline.startup();
            System.out.println("Pard started successfully.");
        }
        catch (Exception e) {
            System.out.println("Pard started failed.");
        }
    }

    private void blockUntilTermination()
    {
        if (server != null) {
            try {
                server.awaitTermination();
            }
            catch (InterruptedException e) {
                stop();
            }
        }
    }

    private void startListener()
    {
        this.serverListener = new ServerListener(configuration.getSocketPort());
        new Thread(serverListener).start();
    }

    private void stop()
    {
        if (server != null) {
            System.out.println("****** Pard shutting down...");
            server.shutdown();
            System.out.println("****** Pard is down");
        }
    }

    public class ServerListener
            implements Runnable
    {
        private final int port;

        ServerListener(int port)
        {
            this.port = port;
        }

        @Override
        public void run()
        {
            try (ServerSocket serverSocket = new ServerSocket(configuration.getSocketPort())) {
                while (true) {
                    Socket socket = serverSocket.accept();
                    new PardQueryHandler(socket).start();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args)
    {
        PardServer server = new PardServer(args[0]);
        server.startup();
    }
}
