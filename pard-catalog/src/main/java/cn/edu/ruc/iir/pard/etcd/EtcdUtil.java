package cn.edu.ruc.iir.pard.etcd;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.GDD;
import cn.edu.ruc.iir.pard.catalog.Privilege;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.catalog.Statics;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.catalog.User;
import cn.edu.ruc.iir.pard.commons.utils.Lib;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.PutResponse;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class EtcdUtil
{
    private static EtcdClient client = new EtcdClient();
    private static Logger log = Logger.getLogger(EtcdUtil.class);
    private static boolean watchStarted = false;
    private static WatchThread etcdSiteWatch = new WatchThread();
    private static WatchThread etcdSchemaWatch = new WatchThread();
    private static WatchThread etcdUserWatch = new WatchThread();

    private EtcdUtil()
    {
    }

    public static void addWatch()
    {
        if (watchStarted) {
            return;
        }
        etcdSiteWatch.setKey("site");
        etcdSchemaWatch.setKey("schema");
        etcdUserWatch.setKey("user");
        etcdSiteWatch.start();
        etcdSchemaWatch.start();
        etcdUserWatch.start();
        watchStarted = true;
        log.info("Etcd watch started");
    }

    public static void stopWatch()
    {
        if (!watchStarted) {
            return;
        }
        etcdSiteWatch.stop();
        etcdSchemaWatch.stop();
        etcdUserWatch.stop();
        watchStarted = false;
    }

    public static EtcdClient getClient()
    {
        return client;
    }

    private static CompletableFuture<PutResponse> putIntKV(KV etcd, String key, int value)
    {
        byte[] bv = Lib.bytesFromInt(value);
        ByteSequence bkey = ByteSequence.fromString(key);
        ByteSequence bvalue = ByteSequence.fromBytes(bv);
        return etcd.put(bkey, bvalue);
    }

    private static CompletableFuture<GetResponse> getIntKV(KV etcd, String key)
    {
        ByteSequence bkey = ByteSequence.fromString(key);
        return etcd.get(bkey);
    }
    private static int parseInt(CompletableFuture<GetResponse> resp)
    {
        try {
            GetResponse res = resp.get();
            List<KeyValue> kv = res.getKvs();
            if (kv.isEmpty()) {
                return 0;
            }
            byte[] bvalue = kv.get(0).getValue().getBytes();
            return Lib.bytesToInt(bvalue, 0);
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return 0;
        }
    }
    public static boolean transGddToEtcd(GDD gdd)
    {
        KV etcd = client.getClient();
        JSONObject jsonObject = JSONObject.fromObject(gdd.getSiteMap());
        ByteSequence key = ByteSequence.fromString("site");
        ByteSequence value = ByteSequence.fromString(jsonObject.toString());
        try {
            putIntKV(etcd, "nextSchemaId", gdd.getNextSchemaId()).get();
            putIntKV(etcd, "nextSiteId", gdd.getNextSiteId()).get();
            putIntKV(etcd, "nextUserId", gdd.getNextUserId()).get();
        }
        catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return false;
        }

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

    public static GDD loadGddFromEtcd()
    {
        GDD gdd = new GDD();
        gdd.setNextSchemaId(parseInt(getIntKV(client.getClient(), "nextSchemaId")));
        gdd.setNextSiteId(parseInt(getIntKV(client.getClient(), "nextSiteId")));
        gdd.setNextUserId(parseInt(getIntKV(client.getClient(), "nextUserId")));
        HashMap<String, Site> siteHashMap = loadSiteFromEtcd();
        HashMap<String, User> userHashMap = loadUserFromEtcd();
        HashMap<String, Schema> schemaHashMap = loadSchemaFromEtcd();
        gdd.setSiteMap(siteHashMap);
        gdd.setSchemaMap(schemaHashMap);
        gdd.setUserMap(userHashMap);
        return gdd;
    }

    private static HashMap<String, Site> loadSiteFromEtcd()
    {
        ByteSequence key = ByteSequence.fromString("site");
        CompletableFuture<GetResponse> getFuture = client.getClient().get(key);
        HashMap<String, Site> siteHashMap = new HashMap<>();
        try {
            GetResponse response = getFuture.get();
            List<KeyValue> list = response.getKvs();
            if (list.isEmpty()) {
                return siteHashMap;
            }
            String sitestr = list.get(0).getValue().toStringUtf8();
            JSONObject jsonObject = JSONObject.fromObject(sitestr);
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Site myobj = (Site) JSONObject.toBean(json, Site.class);
                siteHashMap.put(myobj.getName(), myobj);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return siteHashMap;
    }

    private static HashMap<String, User> loadUserFromEtcd()
    {
        ByteSequence key = ByteSequence.fromString("user");
        CompletableFuture<GetResponse> getFuture = client.getClient().get(key);
        HashMap<String, User> userHashMap = new HashMap<>();
        try {
            GetResponse response = getFuture.get();
            List<KeyValue> list = response.getKvs();
            if (list.isEmpty()) {
                return userHashMap;
            }
            String userstr = list.get(0).getValue().toStringUtf8();
            JSONObject jsonObject = JSONObject.fromObject(userstr);
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                User myobj = (User) JSONObject.toBean(json, User.class);
                myobj = convertUser(myobj);
                userHashMap.put(myobj.getUsername(), myobj);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return userHashMap;
    }

    private static User convertUser(User user)
    {
        JSONObject jsonObject = JSONObject.fromObject(user.getSchemaMap());
        HashMap<String, Privilege> schemaMap = new HashMap<String, Privilege>();
        for (Object obj : jsonObject.keySet()) {
            JSONObject json = (JSONObject) jsonObject.get(obj);
            Privilege plg = (Privilege) JSONObject.toBean(json, Privilege.class);
            schemaMap.put(obj.toString(), plg);
        }
        user.setSchemaMap(schemaMap);
        jsonObject = JSONObject.fromObject(user.getTableMap());
        HashMap<String, Privilege> tableMap = new HashMap<String, Privilege>();
        for (Object obj : jsonObject.keySet()) {
            JSONObject json = (JSONObject) jsonObject.get(obj);
            Privilege plg = (Privilege) JSONObject.toBean(json, Privilege.class);
            tableMap.put(obj.toString(), plg);
        }
        user.setTableMap(tableMap);
        return user;
    }

    private static HashMap<String, Schema> loadSchemaFromEtcd()
    {
        ByteSequence key = ByteSequence.fromString("schema");
        CompletableFuture<GetResponse> getFuture = client.getClient().get(key);
        HashMap<String, Schema> schemaHashMap = new HashMap<String, Schema>();
        HashMap<String, Class> mp = new HashMap<>();
        mp.put("tableList", Table.class);
        mp.put("userList", Privilege.class);
        mp.put("privilegeList", Privilege.class);
        mp.put("condition", Condition.class);
        try {
            GetResponse response = getFuture.get();
            List<KeyValue> list = response.getKvs();
            if (list.isEmpty()) {
                return schemaHashMap;
            }
            String schemastr = list.get(0).getValue().toStringUtf8();
            JSONObject jsonObject = JSONObject.fromObject(schemastr);
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Schema myobj = (Schema) JSONObject.toBean(json, Schema.class, mp);
                myobj = convertSchema(myobj);
                schemaHashMap.put(myobj.getName(), myobj);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return schemaHashMap;
    }

    private static Schema convertSchema(Schema schema)
    {
        List<Table> tableList = schema.getTableList();
        for (Table table : tableList) {
            HashMap<String, Column> columnHashMap = new HashMap<String, Column>();
            HashMap<String, Statics> staticsHashMap = new HashMap<String, Statics>();
            HashMap<String, Fragment> fragmentHashMap = new HashMap<String, Fragment>();
            HashMap<String, Class> mp = new HashMap<String, Class>();
            mp.put("condition", Condition.class);
            JSONObject jsonObject = JSONObject.fromObject(table.getColumns());
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                //System.out.println(json.toString());
                Column col = (Column) JSONObject.toBean(json, Column.class);
                columnHashMap.put(obj.toString(), col);
                if (col.getTableName() == null || col.getTableName().isEmpty()) {
                    col.setTableName(table.getTablename());
                }
            }
            table.setColumns(columnHashMap);
            jsonObject = JSONObject.fromObject(table.getStaticsMap());
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Statics statics = (Statics) JSONObject.toBean(json, Statics.class);
                statics = convertStatic(statics);
                staticsHashMap.put(obj.toString(), statics);
            }
            table.setStaticsMap(staticsHashMap);
            jsonObject = JSONObject.fromObject(table.getFragment());
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Fragment fragment = (Fragment) JSONObject.toBean(json, Fragment.class, mp);
                fragment = convertFragment(fragment);
                fragmentHashMap.put(obj.toString(), fragment);
            }
            table.setFragment(fragmentHashMap);
        }
        return schema;
    }

    private static Fragment convertFragment(Fragment fragment)
    {
        Table table = fragment.getSubTable();
        if (table != null) {
            JSONObject jsonObject = JSONObject.fromObject(table.getColumns());
            HashMap<String, Column> columnHashMap = new HashMap<String, Column>();
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Column col = (Column) JSONObject.toBean(json, Column.class);
                columnHashMap.put(obj.toString(), col);
            }
            table.setColumns(columnHashMap);
            HashMap<String, Statics> staticsHashMap = new HashMap<String, Statics>();
            jsonObject = JSONObject.fromObject(table.getStaticsMap());
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Statics statics = (Statics) JSONObject.toBean(json, Statics.class);
                statics = convertStatic(statics);
                staticsHashMap.put(obj.toString(), statics);
            }
            table.setStaticsMap(staticsHashMap);
            List<Privilege> list = new ArrayList<Privilege>();
            table.setPrivilegeList(list);
            HashMap<String, Fragment> fragmentHashMap = new HashMap<String, Fragment>();
            table.setFragment(fragmentHashMap);
        }
        return fragment;
    }

    private static Statics convertStatic(Statics statics)
    {
        JSONObject jsonObject = JSONObject.fromObject(statics.getStaticList());
        HashMap<String, Integer> staticList = new HashMap<String, Integer>();
        for (Object obj : jsonObject.keySet()) {
            Integer data = Integer.parseInt(jsonObject.get(obj).toString());
           // System.out.println(obj.toString());
            staticList.put(obj.toString(), data);
        }
        statics.setStaticList(staticList);
        return statics;
    }

    public static boolean isSiteChanged()
    {
        return etcdSiteWatch.getFlag();
    }

    public static boolean isSchemaChanged()
    {
        return etcdSchemaWatch.getFlag();
    }

    public static boolean isUserChanged()
    {
        return etcdUserWatch.getFlag();
    }

    public static void setSiteChanged(boolean flag)
    {
        etcdSiteWatch.setFlag(flag);
    }

    public static void setSchemaChanged(boolean flag)
    {
        etcdSchemaWatch.setFlag(flag);
    }

    public static void setUserChanged(boolean flag)
    {
        etcdUserWatch.setFlag(flag);
    }
}
