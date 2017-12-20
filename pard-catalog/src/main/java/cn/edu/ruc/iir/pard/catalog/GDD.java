package cn.edu.ruc.iir.pard.catalog;

import java.util.HashMap;

public class GDD
{
    private HashMap<String, Site> siteMap;
    private HashMap<String, Schema> schemaMap;
    private HashMap<String, User> userMap;
    private int nextSiteId = 1;
    private int nextSchemaId = 1;
    private int nextUserId = 1;
    public GDD()
    {
    }

    public GDD(HashMap<String, Site> siteMap, HashMap<String, Schema> schemaMap, HashMap<String, User> userMap)
    {
        this.siteMap = siteMap;
        this.schemaMap = schemaMap;
        this.userMap = userMap;
    }

    public HashMap<String, Site> getSiteMap()
    {
        return siteMap;
    }

    public void setSiteMap(HashMap<String, Site> siteMap)
    {
        this.siteMap = siteMap;
    }

    public HashMap<String, Schema> getSchemaMap()
    {
        return schemaMap;
    }

    public HashMap<String, User> getUserMap()
    {
        return userMap;
    }

    public void setSchemaMap(HashMap<String, Schema> schemaMap)
    {
        this.schemaMap = schemaMap;
    }

    public void setUserMap(HashMap<String, User> userMap)
    {
        this.userMap = userMap;
    }

    public int getNextSiteId()
    {
        return nextSiteId;
    }

    public int getNextSchemaId()
    {
        return nextSchemaId;
    }

    public int getNextUserId()
    {
        return nextUserId;
    }
    public int nextSiteId()
    {
        ++nextSiteId;
        return nextSiteId;
    }
    public int nextSchemaId()
    {
        nextSchemaId++;
        return nextSchemaId;
    }
    public int nextUserId()
    {
        nextUserId++;
        return nextUserId;
    }
    public void setNextSiteId(int nextSiteId)
    {
        this.nextSiteId = nextSiteId;
    }

    public void setNextSchemaId(int nextSchemaId)
    {
        this.nextSchemaId = nextSchemaId;
    }

    public void setNextUserId(int nextUserId)
    {
        this.nextUserId = nextUserId;
    }
}
