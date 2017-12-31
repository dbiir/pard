package cn.edu.ruc.iir.pard.netty;

import cn.edu.ruc.iir.pard.server.PardNettyServer;
import cn.edu.ruc.iir.pard.server.PardNettyServerHandler;

/**
 * pard
 *
 * @author guodong
 */
public class TestNettyServer
{
    private TestNettyServer()
    {}

    public static void main(String[] args)
    {
        PardNettyServer server = new PardNettyServer(13012);
        PardNettyServerHandler handler = new PardNettyServerHandler();
        server.addAdapter(handler);
        server.run();
    }
}
