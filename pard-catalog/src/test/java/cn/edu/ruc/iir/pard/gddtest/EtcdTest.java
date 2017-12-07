package cn.edu.ruc.iir.pard.gddtest;


import cn.edu.ruc.iir.pard.catalog.*;
import cn.edu.ruc.iir.pard.etcd.EtcdUtil;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.google.gson.JsonArray;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class EtcdTest
{
    @Test
    public GDD createGdd()
    {
        Site site = new Site(1,"crazybird","10.77.40.30",2379,1);
        //Site site=createGdd();
        //JSONObject object = JSONObject.fromObject(site);
        //System.out.println(object.toString());
        HashMap<String, Site>mp=new HashMap<String, Site>();
        mp.put("site1",site);
        mp.put("site2",site);
        mp.put("site3",site);
        HashMap<String, Schema>schemaMap = new HashMap<String, Schema>();
        //userMap.put()
        Schema schema=new Schema();
        schema.setId(1);
        schema.setName("test");
        List<Table>tables=new ArrayList<Table>();
        Table table = new Table();
        table.setTablename("test");
        HashMap<String, Column>columnHashMap=new HashMap<String, Column>();
        Column column = new Column();
        column.setColumnName("pkey");
        column.setDataType(1);
        column.setId(1);
        column.setIndex(1);
        column.setKey(1);
        column.setLen(1);
        columnHashMap.put("pkey",column);
        table.setColumns(columnHashMap);
        table.setFragment(null);
        table.setId(1);
        table.setIsFragment(1);
        table.setPrivilegeList(null);
        table.setStatics(null);
        tables.add(table);
        schema.setTableList(tables);
        schema.setUserList(null);
        schemaMap.put("test",schema);
        GDD gdd = new GDD(mp,schemaMap,null);
        //gdd.setSiteMap(mp);
        return gdd;
    }
    @Test
    public void testJSON()
    {
        GDD gdd = createGdd();
       // JsonArray object = JSONObject.fromObject(gdd);
        //System.out.println(object.toString());
        /*HashMap<String, Site>mp=new HashMap<String, Site>();
        mp.put("site1",site);
        mp.put("site2",site);
        mp.put("site3",site);*/

        JSONObject object = JSONObject.fromObject(gdd.getSchemaMap());
        //System.out.println(object.toString());
        KV client = EtcdUtil.getClient().getClient();
        ByteSequence key = ByteSequence.fromString("schema");
        ByteSequence value = ByteSequence.fromString(object.toString());
        try {
            client.put(key,value).get();
             CompletableFuture<GetResponse> getFuture = client.get(key);
              GetResponse response = getFuture.get();
              List<KeyValue> list = response.getKvs();
              String strname = null;
              for(KeyValue kv : list)
              {
                  System.out.println(kv.getKey().toStringUtf8());
                  //System.out.println(kv.getValue().toStringUtf8());
                  strname=kv.getValue().toStringUtf8();
              }
              System.out.println(strname);
              JSONObject jsonObject = JSONObject.fromObject(strname);
             // JSONObject myobject = (JSONObject) jsonObject.get("siteMap");
              for(Object myobj:jsonObject.keySet()){
                  JSONObject myjson=(JSONObject)jsonObject.get(myobj);
                  System.out.println(myjson.get("test"));
              }
              //System.out.println(myobject.size());
              //System.out.println(myobject.get("site"));

              Map<String, Class> classMap = new HashMap<String, Class>();
              classMap.put("site3",Site.class);
              classMap.put("site1",Site.class);
              classMap.put("site2",Site.class);
             /* List<Site>mp= (List<Site>) JSONArray.toCollection(jsonObject,Site.class);
              //HashMap<String, Site>mp=mygdd.getSiteMap();
              for(Site str:mp){
                 // Site site=(Site) JSONObject.toBean(mp.get(str),Site.class);
                  System.out.println(str.getId());
                  System.out.println(str.getIp());
                  System.out.println(str.getLeader());
                  System.out.println(str.getName());
                  System.out.println(str.getPort());
                  System.out.println("*********************");
              }
              //System.out.println(mp.size());
              //object = JSONObject.fromObject(mygdd);
              //System.out.println(object.toString());*/
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
