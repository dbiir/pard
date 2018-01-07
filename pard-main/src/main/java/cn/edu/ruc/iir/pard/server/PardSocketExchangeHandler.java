package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.EORTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardSocketExchangeHandler
        extends Thread
{
    private final Socket socket;
    private final PardTaskExecutor executor;
    private final Logger logger = Logger.getLogger(PardSocketExchangeHandler.class.getName());

    public PardSocketExchangeHandler(Socket socket, PardTaskExecutor executor)
    {
        this.socket = socket;
        this.executor = executor;
    }

    @Override
    public void run()
    {
        try (ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream())) {
            while (true) {
                Task task = (Task) inputStream.readObject();
                if (task instanceof EORTask) {
                    logger.info("Exchange handler session out");
                    break;
                }
                Block block = executor.execute(task);
                outputStream.writeObject(block);
                logger.info("One round interaction done");
            }
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
