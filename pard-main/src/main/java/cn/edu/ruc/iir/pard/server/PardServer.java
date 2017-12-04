package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.communication.rpc.PardRPCService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;

/**
 * pard
 *
 * @author guodong
 */
public class PardServer
{
    private final PardUserConfiguration configuration;

    private Server server;

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
                    try {
                        server = serverBuilder.build();
                        server.start();
                        System.out.println("RPC Server Started");
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });

        pipeline.addStartupHook(
                () -> Runtime.getRuntime().addShutdownHook(
                        new Thread(PardServer.this::stop)));

        // add server running in loop hook
        pipeline.addStartupHook(
                this::blockUntilTermination);

        try {
            pipeline.startup();
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

    private void stop()
    {
        if (server != null) {
            System.out.println("****** Pard shutting down...");
            server.shutdown();
            System.out.println("****** Pard is down");
        }
    }

    public static void main(String[] args)
    {
        PardServer server = new PardServer(args[0]);
        server.startup();
    }
}
