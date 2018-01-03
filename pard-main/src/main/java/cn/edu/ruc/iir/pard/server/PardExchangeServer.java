package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.executor.connector.Connector;

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
    private final Connector connector;
    private boolean stopFlag = false;

    public PardExchangeServer(int port, Connector connector)
    {
        this.port = port;
        this.connector = connector;
    }

    @Override
    public void run()
    {
        logger.info("Exchange server started at port: " + port);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (!stopFlag) {
                Socket socket = serverSocket.accept();
                executorService.submit(new PardExchangeHandler(socket, connector));
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
