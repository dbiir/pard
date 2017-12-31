package cn.edu.ruc.iir.pard.catalog;

public class Privilege
{
    private int use; //1,read, 3, write,5create 7,delete;
    private int uid;
    private String username;
    public Privilege()
    {
    }
    public Privilege(int use, int uid, String username)
    {
        this.use = use;
        this.uid = uid;
        this.username = username;
    }

    public int getUse()
    {
        return use;
    }

    public int getUid()
    {
        return uid;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUse(int use)
    {
        this.use = use;
    }

    public void setUid(int uid)
    {
        this.uid = uid;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }
}
