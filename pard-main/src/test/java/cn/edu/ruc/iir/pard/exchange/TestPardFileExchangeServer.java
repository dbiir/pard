package cn.edu.ruc.iir.pard.exchange;

/**
 * pard
 *
 * @author guodong
 */
public class TestPardFileExchangeServer
{
    private TestPardFileExchangeServer()
    {}

    public static void main(String[] args)
    {
        PardFileExchangeServer fileExchangeServer = new PardFileExchangeServer(10012, null);
        fileExchangeServer.run();
    }
}
