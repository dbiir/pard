package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.connector.postgresql.PostgresConnector;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.nodekeeper.Keeper;
import cn.edu.ruc.iir.pard.scheduler.JobScheduler;
import cn.edu.ruc.iir.pard.scheduler.TaskScheduler;

/**
 * pard
 * Happy new year
 *
 * @author guodong
 */
public class PardServer
{
    private final PardUserConfiguration configuration;
    private PardRPCServer rpcServer;
    private PardSocketListener socketListener;
    private Connector connector;
    private Keeper keeper;
    private JobScheduler jobScheduler;
    private TaskScheduler taskScheduler;

    PardServer(String configurationPath)
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

        // load node keeper
        pipeline.addStartupHook(this::loadNodeKeeper);

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
            System.out.println("Pard started failed.");
        }
    }

    private void registerNode()
    {
        SiteDao siteDao = new SiteDao();
        Site currentSite = new Site();
        currentSite.setName(configuration.getNodeName());
        siteDao.add(currentSite, true);
    }

    private void loadConnector()
    {
        this.connector = PostgresConnector.INSTANCE();
    }

    private void startRPCServer()
    {
        PardRPCServer rpcServer = new PardRPCServer(configuration.getRPCPort(), connector);
        new Thread(rpcServer).start();
    }

    private void startExchangeServer()
    {
        PardExchangeServer exchangeServer = new PardExchangeServer(configuration.getSocketPort(), connector);
    }

    private void startSocketListener()
    {
        PardSocketListener socketListener = new PardSocketListener(configuration.getSocketPort(),
                jobScheduler, taskScheduler);
        new Thread(socketListener).start();
    }

    private void loadNodeKeeper()
    {
        this.keeper = Keeper.INSTANCE();
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
        System.out.println("****** Pard is down");
    }

    public static void main(String[] args)
    {
        PardServer server = new PardServer(args[0]);
        server.startup();
    }
}
