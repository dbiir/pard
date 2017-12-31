package cn.edu.ruc.iir.pard.semantic;

import cn.edu.ruc.iir.pard.catalog.GDD;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.etcd.EtcdUtil;

public class CatalogUtil
{
    private GDD gdd = null;
    private static volatile CatalogUtil instance = null;
    private static Object o = new Object();
    public static CatalogUtil getInstance()
    {
        if (instance == null) {
            synchronized (o) {
                if (instance == null) {
                    synchronized (o) {
                        instance = new CatalogUtil();
                    }
                }
            }
        }
        return instance;
    }
    private CatalogUtil()
    {
        gdd = refreshGDD();
    }
    public GDD refreshGDD()
    {
        return EtcdUtil.loadGddFromEtcd();
    }
    public Schema getSchema(String name)
    {
        return gdd.getSchemaMap().get(name);
    }
}
