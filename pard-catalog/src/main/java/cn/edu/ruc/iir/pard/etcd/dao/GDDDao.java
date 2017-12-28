package cn.edu.ruc.iir.pard.etcd.dao;

import cn.edu.ruc.iir.pard.catalog.GDD;
import cn.edu.ruc.iir.pard.etcd.EtcdUtil;
/**
 * pard: GDDDao
 * The data access object of GDD.
 * used for GDD's load & persist.
 *
 * @author hagen
 * */
public class GDDDao
{
    private static GDD gdd = null;
    public GDDDao(){}
    public GDD load()
    {
        gdd = EtcdUtil.loadGddFromEtcd();
        return gdd;
    }
    public boolean persist()
    {
        return EtcdUtil.transGddToEtcd(gdd);
    }
    public boolean persistGDD(GDD gdd)
    {
        return EtcdUtil.transGddToEtcd(gdd);
    }
}
