package cn.edu.ruc.iir.pard.etcd;

//import com.coreos.jetcd.KV;

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
}
