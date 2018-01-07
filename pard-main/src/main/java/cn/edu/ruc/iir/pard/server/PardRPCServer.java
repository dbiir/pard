package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.communication.rpc.PardRPCService;
import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
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
    private final PardTaskExecutor executor;
    private Server server;       // rpc server

    public PardRPCServer(int port, PardTaskExecutor executor)
    {
        this.port = port;
        this.executor = executor;
    }

    @Override
    public void run()
    {
        ServerBuilder<NettyServerBuilder> serverBuilder =
                NettyServerBuilder.forPort(port);
        serverBuilder.addService(new PardRPCService(executor));
        server = serverBuilder.build();
        try {
            server.start();
            logger.info("RPC server started at port " + port);
            server.awaitTermination();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("RPC server shut down");
    }

    public void stop()
    {
        server.shutdown();
        logger.info("Pard rpc server stopped");
    }
}
