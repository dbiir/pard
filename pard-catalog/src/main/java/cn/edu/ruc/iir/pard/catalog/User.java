package cn.edu.ruc.iir.pard.catalog;

import java.util.HashMap;

public class User
{
    private int uid;
    private String username;
    private HashMap<String, Privilege> tableMap; //table name and then privilege, which the user have
    private HashMap<String, Privilege> schemaMap; //schema name and then privilege
    public User()
    {
    }

    public User(int uid, String username, HashMap<String, Privilege> tableMap, HashMap<String, Privilege> schemaMap)
    {
        this.uid = uid;
        this.username = username;
        this.tableMap = tableMap;
        this.schemaMap = schemaMap;
    }

    public int getUid()
    {
        return uid;
    }

    public String getUsername()
    {
        return username;
    }

    public HashMap<String, Privilege> getTableMap()
    {
        return tableMap;
    }

    public HashMap<String, Privilege> getSchemaMap()
    {
        return schemaMap;
    }

    public void setUid(int uid)
    {
        this.uid = uid;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public void setTableMap(HashMap<String, Privilege> tableMap)
    {
        this.tableMap = tableMap;
    }

    public void setSchemaMap(HashMap<String, Privilege> schemaMap)
    {
        this.schemaMap = schemaMap;
    }
}
