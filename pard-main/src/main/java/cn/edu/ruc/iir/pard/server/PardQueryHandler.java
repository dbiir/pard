package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.utils.ResultSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardQueryHandler
        extends Thread
{
    private Socket socket;
    private Logger logger = Logger.getLogger("pard server");
    private ObjectOutputStream objectOutputStream;
    {
        try {
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public PardQueryHandler(Socket socket)
    {
        this.socket = socket;
    }

    @Override
    public void run()
    {
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))) {
            while (true) {
                String line = input.readLine();
                if (line.equalsIgnoreCase("EXIT") ||
                        line.equalsIgnoreCase("QUIT")) {
                    logger.info("CLIENT QUIT");
                    break;
                }
                logger.info("QUERY: " + line);
                ResultSet result = executeQuery(line);
                objectOutputStream.writeObject(result);
                objectOutputStream.flush();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ResultSet executeQuery(String sql)
    {
        // execute query
        logger.info("Executing query: " + sql);
        return new ResultSet(ResultSet.ResultStatus.OK);
    }
}
