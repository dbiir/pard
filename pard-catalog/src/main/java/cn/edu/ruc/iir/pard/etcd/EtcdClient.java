package cn.edu.ruc.iir.pard.etcd;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Watch;
//import com.coreos.jetcd.data.ByteSequence;

public class EtcdClient
{
    private static String url = "http://10.77.40.41:2379";
    private KV client;
    private Watch watch;
    public EtcdClient()
    {
        Client client = Client.builder().endpoints(url).build();
        this.client = client.getKVClient();
        this.watch = client.getWatchClient();
        //ByteSequence key = ByteSequence.fromString("test_key");
        //ByteSequence value = ByteSequence.fromString("test_value");
        //kvClient.put(key, value).get();
        //CompletableFuture<GetResponse> getFuture = kvClient.get(key);
        //GetResponse response = getFuture.get();
        //DeleteResponse deleteRangeResponse = kvClient.delete(key).get();
    }
    public KV getClient()
    {
        return this.client;
    }
    public Watch getWatch()
    {
        return this.watch;
    }
}
