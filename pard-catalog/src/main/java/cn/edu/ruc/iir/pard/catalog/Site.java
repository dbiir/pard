package cn.edu.ruc.iir.pard.catalog;

public class Site
{
    private int id;
    private String name;
    private String ip;
    private int port;
    private int leader;
    public Site()
    {
    }

    public Site(int id, String name, String ip, int port, int leader)
    {
        this.id = id;
        this.name = name;
        this.ip = ip;
        this.port = port;
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

    public void setPort(int port)
    {
        this.port = port;
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

    public int getPort()
    {
        return port;
    }

    public int getLeader()
    {
        return leader;
    }
}
