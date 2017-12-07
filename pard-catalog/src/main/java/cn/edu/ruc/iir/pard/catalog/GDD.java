package cn.edu.ruc.iir.pard.catalog;

import java.util.HashMap;

public class GDD
{
    HashMap<String, Site> siteMap;
    HashMap<String, Schema> schemaMap;
    HashMap<String, User> userMap;
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
}
