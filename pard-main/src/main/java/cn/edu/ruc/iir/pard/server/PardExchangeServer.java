package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.executor.connector.Connector;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * pard
 *
 * @author guodong
 */
public class PardExchangeServer
        implements Runnable
{
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
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (!stopFlag) {
                Socket socket = serverSocket.accept();
                PardExchangeHandler handler = new PardExchangeHandler(socket, connector);
                handler.run();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
