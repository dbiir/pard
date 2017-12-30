package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.scheduler.JobScheduler;
import cn.edu.ruc.iir.pard.scheduler.TaskScheduler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardSocketListener
        implements Runnable
{
    private final int port;
    private final JobScheduler jobScheduler;
    private final TaskScheduler taskScheduler;
    private boolean stopFlag = false;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Logger logger = Logger.getLogger(PardSocketListener.class.getName());

    PardSocketListener(int port, JobScheduler jobScheduler, TaskScheduler taskScheduler)
    {
        this.port = port;
        this.jobScheduler = jobScheduler;
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void run()
    {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            logger.info("Pard socket server started at port " + port);
            while (!stopFlag) {
                Socket socket = serverSocket.accept();
                executorService.submit(new PardQueryHandler(socket, jobScheduler, taskScheduler));
//                new PardQueryHandler(socket).start();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop()
    {
        this.stopFlag = true;
        executorService.shutdown();
        logger.info("Pard socket listener stopped");
    }
}
