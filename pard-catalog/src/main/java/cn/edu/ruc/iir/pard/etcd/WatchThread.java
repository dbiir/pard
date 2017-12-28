package cn.edu.ruc.iir.pard.etcd;

import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.watch.WatchResponse;

public class WatchThread
        extends Thread
{
    private boolean flag;
    private Watch watch;
    private String key;
    private Watch.Watcher watcher;
    public WatchThread()
    {
        flag = false;
        this.watch = EtcdUtil.getClient().getWatch();
    }
    public WatchThread(String key)
    {
        flag = false;
        this.watch = EtcdUtil.getClient().getWatch();
        this.key = key;
        watcher = watch.watch(ByteSequence.fromString(key));
    }
    public void setKey(String key)
    {
        this.key = key;
        watcher = watch.watch(ByteSequence.fromString(key));
    }
    public String getKey()
    {
        return this.key;
    }
    @Override
    public void run()
    {
        while (true) {
            try {
                WatchResponse response = watcher.listen();
                flag = true;
                /*for (WatchEvent event : response.getEvents())
                {
                    System.out.println(event.getKeyValue().getKey().toStringUtf8());
                    System.out.println(event.getKeyValue().getValue().toStringUtf8());
                }*/
            }
            catch (Exception e) {
                System.out.println("Etcd:get watch response failure!");
                e.printStackTrace();
                flag = false;
                break;
            }
        }
    }
    public void setFlag(boolean flag)
    {
        this.flag = flag;
    }
    public boolean getFlag()
    {
        return this.flag;
    }
}
