package cn.edu.ruc.iir.pard.nodekeeper;

public class ServerInfo
{
    private String name = null;
    private String ip = null;
    private int port = 0;
    private int status = 0;

    public ServerInfo()
    {
    }

    public ServerInfo(String name, String ip, int port)
    {
        super();
        this.name = name;
        this.ip = ip;
        this.port = port;
    }
    public String getName()
    {
        return name;
    }
    public void setName(String name)
    {
        this.name = name;
    }
    public String getIp()
    {
        return ip;
    }
    public void setIp(String ip)
    {
        this.ip = ip;
    }
    public int getPort()
    {
        return port;
    }
    public void setPort(int port)
    {
        this.port = port;
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
