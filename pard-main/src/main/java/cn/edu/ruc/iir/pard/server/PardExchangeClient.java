package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
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
public class PardExchangeClient
{
    private final Logger logger = Logger.getLogger(PardExchangeClient.class.getName());
    private final ObjectInputStream inputStream;
    private final ObjectOutputStream outputStream;
    private final Socket socket;

    public PardExchangeClient(String host, int port) throws IOException
    {
        this.socket = new Socket(host, port);
        this.outputStream = new ObjectOutputStream(socket.getOutputStream());
        this.inputStream = new ObjectInputStream(socket.getInputStream());
    }

    public PardResultSet call(Task task) throws IOException, ClassNotFoundException
    {
        logger.info("Exchange client calling task: " + task);
        outputStream.writeObject(task);
        outputStream.flush();
        return (PardResultSet) inputStream.readObject();
    }

    public void close() throws IOException
    {
        if (socket != null) {
            this.socket.close();
            this.inputStream.close();
            this.outputStream.close();
        }
    }
}
