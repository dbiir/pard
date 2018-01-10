package cn.edu.ruc.iir.pard.catalog;
/**
 * site
 * We only need id,name,status
 *
 * @author hagen
 * */
public class Site
{
    private int id;
    private String name;
    private String ip;
    private int serverPort;
    private int rpcPort;
    private int exchangePort;
    private int fileExchangePort;
    private int leader;
    private int status;
    public Site()
    {
    }

    public Site(int id, String name, String ip, int serverPort, int leader)
    {
        this.id = id;
        this.name = name;
        this.ip = ip;
        this.serverPort = serverPort;
        this.leader = leader;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setIp(String ip)
    {
        this.ip = ip;
    }

    public void setServerPort(int serverPort)
    {
        this.serverPort = serverPort;
    }

    public int getRpcPort()
    {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort)
    {
        this.rpcPort = rpcPort;
    }

    public int getExchangePort()
    {
        return exchangePort;
    }

    public void setExchangePort(int exchangePort)
    {
        this.exchangePort = exchangePort;
    }

    public void setFileExchangePort(int fileExchangePort)
    {
        this.fileExchangePort = fileExchangePort;
    }

    public int getFileExchangePort()
    {
        return fileExchangePort;
    }

    public void setLeader(int leader)
    {
        this.leader = leader;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getIp()
    {
        return ip;
    }

    public int getServerPort()
    {
        return serverPort;
    }

    public int getLeader()
    {
        return leader;
    }

    public int getStatus()
    {
        return status;
    }

    public void setStatus(int status)
    {
        this.status = status;
    }
}
