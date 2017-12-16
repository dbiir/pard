package cn.edu.ruc.iir.pard.etcd.dao;

import cn.edu.ruc.iir.pard.catalog.GDD;
import cn.edu.ruc.iir.pard.catalog.Site;
/**
 * pard
 * SiteDao
 * provide interface for Site's CURD
 *
 * @author hagen
 * */
import java.util.Map;

public class SiteDao
        extends GDDDao
{
    public SiteDao(){}
    public Site loadByName(String name)
    {
        GDD gdd = load();
        return gdd.getSiteMap().get(name);
    }
    public boolean update(Site site)
    {
        GDD gdd = load();
        Map<String, Site> siteMap = gdd.getSiteMap();
        siteMap.put(site.getName(), site);
        return persistGDD(gdd);
    }
    public boolean add(Site site, boolean check)
    {
        GDD gdd = load();
        Map<String, Site> siteMap = gdd.getSiteMap();
        if (check) {
            if (siteMap.get(site.getName()) != null) {
                return false;
            }
        }
        site.setId(gdd.nextSiteId());
        siteMap.put(site.getName(), site);
        return persistGDD(gdd);
    }
    public boolean drop(String name)
    {
        GDD gdd = load();
        Map<String, Site> siteMap = gdd.getSiteMap();
        siteMap.put(name, null);
        return persistGDD(gdd);
    }
}
