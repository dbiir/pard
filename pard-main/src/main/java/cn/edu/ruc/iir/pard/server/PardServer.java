package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;

/**
 * pard
 *
 * @author guodong
 */
public class PardServer
{
    private final PardUserConfiguration configuration;
    private PardRPCServer rpcServer;
    private PardSocketListener socketListener;

    PardServer(String configurationPath)
    {
        this.configuration = PardUserConfiguration.INSTANCE();
        configuration.init(configurationPath);
    }

    private void startup()
    {
        PardStartupPipeline pipeline = new PardStartupPipeline();

        // todo validate configuration first

        // start rpc server
        pipeline.addStartupHook(this::startRPCServer);

        pipeline.addStartupHook(
                () -> Runtime.getRuntime().addShutdownHook(
                        new Thread(PardServer.this::stop)));

        // start socket listener
        pipeline.addStartupHook(this::startSocketListener);

        try {
            pipeline.startup();
            System.out.println("Pard started successfully.");
        }
        catch (Exception e) {
            System.out.println("Pard started failed.");
        }
    }

    private void startRPCServer()
    {
        PardRPCServer rpcServer = new PardRPCServer(configuration.getRPCPort());
        new Thread(rpcServer).start();
    }

    private void startSocketListener()
    {
        PardSocketListener socketListener = new PardSocketListener(configuration.getSocketPort());
        new Thread(socketListener).start();
    }

    private void stop()
    {
        System.out.println("****** Pard shutting down...");
        socketListener.stop();
        rpcServer.stop();
        System.out.println("****** Pard is down");
    }

    public static void main(String[] args)
    {
        PardServer server = new PardServer(args[0]);
        server.startup();
    }
}
