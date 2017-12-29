package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.communication.rpc.PardRPCService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardRPCServer
        implements Runnable
{
    private final Logger logger = Logger.getLogger(PardRPCServer.class.getName());
    private final int port;
    private Server server;       // rpc server

    public PardRPCServer(int port)
    {
        this.port = port;
    }

    @Override
    public void run()
    {
        ServerBuilder<NettyServerBuilder> serverBuilder =
                NettyServerBuilder.forPort(port);
        serverBuilder.addService(new PardRPCService());
        server = serverBuilder.build();
        try {
            server.start();
            logger.info("RPC server started at port " + port);
            server.awaitTermination();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop()
    {
        server.shutdown();
        logger.info("Pard rpc server stopped");
    }
}
