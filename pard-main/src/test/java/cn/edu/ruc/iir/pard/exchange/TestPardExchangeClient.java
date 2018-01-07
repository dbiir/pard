package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.TestTask;

/**
 * pard
 *
 * @author guodong
 */
public class TestPardExchangeClient
{
    private TestPardExchangeClient()
    {}

    public static void main(String[] args)
    {
        PardExchangeClient exchangeClient = new PardExchangeClient("127.0.0.1", 10012);
        TestTask testTask = new TestTask("");
        exchangeClient.connect(testTask);
    }
}
