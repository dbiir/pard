package cn.edu.ruc.iir.pard.etcd;

//import com.coreos.jetcd.KV;

import cn.edu.ruc.iir.pard.catalog.GDD;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import net.sf.json.JSONObject;

public class EtcdUtil
{
    private static EtcdClient client = new EtcdClient();
    private EtcdUtil()
    {
        if (client == null) {
            client = new EtcdClient();
        }
    }
    public static EtcdClient getClient()
    {
        return client;
    }
    public static boolean TransGddToEtcd(GDD gdd)
    {
        KV etcd = client.getClient();
        JSONObject jsonObject = JSONObject.fromObject(gdd.getSiteMap());
        ByteSequence key = ByteSequence.fromString("site");
        ByteSequence value = ByteSequence.fromString(jsonObject.toString());
        try {
            etcd.put(key, value).get();
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        jsonObject = JSONObject.fromObject(gdd.getSchemaMap());
        key = ByteSequence.fromString("schema");
        value = ByteSequence.fromString(jsonObject.toString());
        try {
            etcd.put(key, value).get();
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        jsonObject = JSONObject.fromObject(gdd.getUserMap());
        key = ByteSequence.fromString("user");
        value = ByteSequence.fromString(jsonObject.toString());
        try {
            etcd.put(key, value).get();
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
