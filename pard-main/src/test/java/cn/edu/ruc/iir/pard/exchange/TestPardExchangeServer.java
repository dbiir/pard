package cn.edu.ruc.iir.pard.exchange;

/**
 * pard
 *
 * @author guodong
 */
public class TestPardExchangeServer
{
    private TestPardExchangeServer()
    {}

    public static void main(String[] args)
    {
        PardExchangeServer exchangeServer = new PardExchangeServer(10012, null);
        exchangeServer.run();
    }
}
