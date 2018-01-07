package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;

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
public class PardExchangeServer
        implements Runnable
{
    private final Logger logger = Logger.getLogger(PardExchangeServer.class.getName());
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final int port;
    private final PardTaskExecutor executor;
    private boolean stopFlag = false;

    public PardExchangeServer(int port, PardTaskExecutor executor)
    {
        this.port = port;
        this.executor = executor;
    }

    @Override
    public void run()
    {
        logger.info("Exchange server started at port: " + port);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (!stopFlag) {
                Socket socket = serverSocket.accept();
                executorService.submit(new PardExchangeHandler(socket, executor));
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exchange server shut down");
    }

    public void stop()
    {
        this.stopFlag = true;
    }
}
