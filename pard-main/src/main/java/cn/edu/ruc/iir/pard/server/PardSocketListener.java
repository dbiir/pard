package cn.edu.ruc.iir.pard.server;

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
    private boolean stopFlag = false;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Logger logger = Logger.getLogger(PardSocketListener.class.getName());

    PardSocketListener(int port)
    {
        this.port = port;
    }

    @Override
    public void run()
    {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (!stopFlag) {
                Socket socket = serverSocket.accept();
                executorService.submit(new PardQueryHandler(socket));
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
