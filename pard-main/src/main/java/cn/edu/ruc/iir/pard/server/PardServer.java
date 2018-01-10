package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.connector.postgresql.PostgresConnector;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.exchange.PardExchangeServer;
import cn.edu.ruc.iir.pard.exchange.PardFileExchangeServer;
import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.scheduler.JobScheduler;
import cn.edu.ruc.iir.pard.scheduler.TaskScheduler;

/**
 * pard main server
 *
 * @author guodong
 */
public class PardServer
{
    private final PardUserConfiguration configuration;
    private PardRPCServer rpcServer;
    private PardSocketListener socketListener;
    private PardExchangeServer exchangeServer;
    private PardFileExchangeServer fileExchangeServer;
    private Connector connector;
    private PardTaskExecutor executor;
    private JobScheduler jobScheduler;
    private TaskScheduler taskScheduler;

    private PardServer(String configurationPath)
    {
        this.configuration = PardUserConfiguration.INSTANCE();
        configuration.init(configurationPath);
    }

    private void startup()
    {
        PardStartupPipeline pipeline = new PardStartupPipeline();

        // todo validate configuration first

        // register node
        pipeline.addStartupHook(this::registerNode);

        // load connector
        pipeline.addStartupHook(this::loadConnector);

        // load executor
        pipeline.addStartupHook(this::loadExecutor);

//         load node keeper
//        pipeline.addStartupHook(this::loadNodeKeeper);

        // start exchange server
        pipeline.addStartupHook(this::startExchangeServer);

        // start file exchange server
        pipeline.addStartupHook(this::startFileExchangeServer);

        // load job scheduler
        pipeline.addStartupHook(this::loadJobScheduler);

        // load task scheduler
        pipeline.addStartupHook(this::loadTaskScheduler);

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
            e.printStackTrace();
            System.out.println("Pard started failed.");
            System.exit(-1);
        }
    }

    private void registerNode()
    {
        SiteDao siteDao = new SiteDao();
        Site currentSite = new Site();
        currentSite.setName(configuration.getNodeName());
        currentSite.setServerPort(configuration.getServerPort());
        currentSite.setIp(configuration.getHost());
        currentSite.setRpcPort(configuration.getRPCPort());
        currentSite.setExchangePort(configuration.getExchangePort());
        currentSite.setFileExchangePort(configuration.getFileExchangePort());
        currentSite.setServerPort(configuration.getServerPort());
        siteDao.add(currentSite, false);
    }

    private void loadConnector()
    {
        this.connector = PostgresConnector.INSTANCE();
    }

    private void loadExecutor()
    {
        this.executor = PardTaskExecutor.INSTANCE();
        executor.setConnector(connector);
    }

    private void startRPCServer()
    {
        this.rpcServer = new PardRPCServer(configuration.getRPCPort(), executor);
        new Thread(rpcServer).start();
    }

    private void startExchangeServer()
    {
//        this.exchangeServer = new PardSocketExchangeServer(configuration.getExchangePort(), executor);
        this.exchangeServer = new PardExchangeServer(configuration.getExchangePort(), executor);
        new Thread(exchangeServer).start();
    }

    private void startFileExchangeServer()
    {
        this.fileExchangeServer = new PardFileExchangeServer(configuration.getFileExchangePort(), executor);
        new Thread(fileExchangeServer).start();
    }

    private void startSocketListener()
    {
        this.socketListener = new PardSocketListener(configuration.getServerPort(),
                jobScheduler, taskScheduler);
        new Thread(socketListener).start();
    }

    private void loadJobScheduler()
    {
        this.jobScheduler = JobScheduler.INSTANCE();
    }

    private void loadTaskScheduler()
    {
        this.taskScheduler = TaskScheduler.INSTANCE();
    }

    private void stop()
    {
        System.out.println("****** Pard shutting down...");
        socketListener.stop();
        rpcServer.stop();
        exchangeServer.stop();
        fileExchangeServer.stop();
        connector.close();
        System.out.println("****** Pard is down");
    }

    public static void main(String[] args)
    {
        PardServer server = new PardServer(args[0]);
        server.startup();
    }
}
