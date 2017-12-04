package cn.edu.ruc.iir.pard.communication.rpc;

import org.testng.annotations.Test;

/**
 * pard
 *
 * @author guodong
 */
public class PardRPCTest
{
    PardRPCClient client = new PardRPCClient("127.0.0.1", 10012);

    @Test
    public void rpcHeartbeatTest()
    {
        int type = client.sendHeartBeat(1, 0, "hello fellow, this is 0");
        assert type == 2;
    }
}
