package cn.edu.ruc.iir.pard.netty;

import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.server.PardNettyClient;
import cn.edu.ruc.iir.pard.server.PardNettyClientHandler;

/**
 * pard
 *
 * @author guodong
 */
public class TestNettyClient
{
    private TestNettyClient()
    {}

    public static void main(String[] args)
    {
        PardNettyClient client = new PardNettyClient("127.0.0.1", 13012);
        PardNettyClientHandler handler = new PardNettyClientHandler();
        Task task = new CreateSchemaTask("i", true, "pard0");
        task.setTaskId("task0");
        client.addAdapter(handler);
        client.setTask(task);
        client.run();
    }
}
