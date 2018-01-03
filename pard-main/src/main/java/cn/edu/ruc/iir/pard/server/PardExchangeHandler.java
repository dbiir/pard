package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
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
public class PardExchangeHandler
        extends Thread
{
    private final Socket socket;
    private final Connector connector;
    private final Logger logger = Logger.getLogger(PardExchangeHandler.class.getName());

    public PardExchangeHandler(Socket socket, Connector connector)
    {
        this.socket = socket;
        this.connector = connector;
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
                PardResultSet resultSet = connector.execute(task);
                outputStream.writeObject(resultSet);
                logger.info("One round interaction done");
            }
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
