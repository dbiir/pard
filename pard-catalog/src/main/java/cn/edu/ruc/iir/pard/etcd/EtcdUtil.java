package cn.edu.ruc.iir.pard.etcd;

//import com.coreos.jetcd.KV;

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
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
    public static GDD LoadGddFromEtcd()
    {
        GDD gdd = new GDD();
        HashMap<String, Site> siteHashMap = LoadSiteFromEtcd();
        for (String name : siteHashMap.keySet()) {
           //  System.out.println(siteHashMap.get(name).getName());
        }
        HashMap<String, User> userHashMap = LoadUserFromEtcd();
        for (String name : userHashMap.keySet()) {
            HashMap<String, Privilege> schemaMap = userHashMap.get(name).getSchemaMap();
            for (String sname : schemaMap.keySet()) {
                // System.out.println(schemaMap.get(sname).getUse());
            }
            HashMap<String, Privilege> tableMap = userHashMap.get(name).getTableMap();
            for (String sname : tableMap.keySet()) {
                //System.out.println(tableMap.get(sname).getUsername());
            }
        }
        HashMap<String, Schema> schemaHashMap = LoadSchemaFromEtcd();
        gdd.setSiteMap(siteHashMap);
        gdd.setSchemaMap(schemaHashMap);
        gdd.setUserMap(userHashMap);
        return gdd;
    }
    public static HashMap<String, Site> LoadSiteFromEtcd()
    {
        ByteSequence key = ByteSequence.fromString("site");
        CompletableFuture<GetResponse> getFuture = client.getClient().get(key);
        HashMap<String, Site> siteHashMap = new HashMap<String, Site>();
        try {
            GetResponse response = getFuture.get();
            List<KeyValue> list = response.getKvs();
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
    public static HashMap<String, User> LoadUserFromEtcd()
    {
        ByteSequence key = ByteSequence.fromString("user");
        CompletableFuture<GetResponse> getFuture = client.getClient().get(key);
        HashMap<String, User> userHashMap = new HashMap<String, User>();
        try {
            GetResponse response = getFuture.get();
            List<KeyValue> list = response.getKvs();
            String userstr = list.get(0).getValue().toStringUtf8();
            JSONObject jsonObject = JSONObject.fromObject(userstr);
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                User myobj = (User) JSONObject.toBean(json, User.class);
                myobj = ConvertUser(myobj);
                userHashMap.put(myobj.getUsername(), myobj);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return userHashMap;
    }
    public static User ConvertUser(User user)
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
    public static HashMap<String, Schema> LoadSchemaFromEtcd()
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
            String schemastr = list.get(0).getValue().toStringUtf8();
            JSONObject jsonObject = JSONObject.fromObject(schemastr);
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Schema myobj = (Schema) JSONObject.toBean(json, Schema.class, mp);
                myobj = ConvertSchema(myobj);
                schemaHashMap.put(myobj.getName(), myobj);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return schemaHashMap;
    }
    public static Schema ConvertSchema(Schema schema)
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
            }
            table.setColumns(columnHashMap);
            jsonObject = JSONObject.fromObject(table.getStaticsMap());
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Statics statics = (Statics) JSONObject.toBean(json, Statics.class);
                statics = ConvertStatic(statics);
                staticsHashMap.put(obj.toString(), statics);
            }
            table.setStaticsMap(staticsHashMap);
            jsonObject = JSONObject.fromObject(table.getFragment());
            for (Object obj : jsonObject.keySet()) {
                JSONObject json = (JSONObject) jsonObject.get(obj);
                Fragment fragment = (Fragment) JSONObject.toBean(json, Fragment.class, mp);
                fragment = ConvertFragment(fragment);
                fragmentHashMap.put(obj.toString(), fragment);
            }
            table.setFragment(fragmentHashMap);
        }
        return schema;
    }
    public static Fragment ConvertFragment(Fragment fragment)
    {
        Table table = fragment.getSubTable();
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
            statics = ConvertStatic(statics);
            staticsHashMap.put(obj.toString(), statics);
        }
        table.setStaticsMap(staticsHashMap);
        List<Privilege> list = new ArrayList<Privilege>();
        table.setPrivilegeList(list);
        HashMap<String, Fragment> fragmentHashMap = new HashMap<String, Fragment>();
        table.setFragment(fragmentHashMap);
        return fragment;
    }
    public static Statics ConvertStatic(Statics statics)
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
}
