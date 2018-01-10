package cn.edu.ruc.iir.pard.exchange;

/**
 * pard
 *
 * @author guodong
 */
public class TestPardFileExchangeClient
{
    private TestPardFileExchangeClient()
    {}

    public static void main(String[] args)
    {
        PardFileExchangeClient fileExchangeClient = new PardFileExchangeClient("127.0.0.1", 10012, "/Users/Jelly/Desktop/emp.tsv", "pard", "emp", "", null);
        fileExchangeClient.run();
    }
}
