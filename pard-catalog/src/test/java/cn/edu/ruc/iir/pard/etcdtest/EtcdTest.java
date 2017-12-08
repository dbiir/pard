package cn.edu.ruc.iir.pard.etcdtest;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.GDD;
import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.catalog.Privilege;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.catalog.Statics;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.catalog.User;

import cn.edu.ruc.iir.pard.etcd.EtcdUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EtcdTest
{
    @Test
    public void TestEtcd()
    {
        EtcdUtil.TransGddToEtcd(CreateGdd());
    }
    @Test
    public GDD CreateGdd()
    {
        GDD gdd = new GDD();
        gdd.setSiteMap(CreateSite());
        gdd.setSchemaMap(CreateSchema());
        gdd.setUserMap(CreateUser());
        return gdd;
    }
    public HashMap<String, User> CreateUser()
    {
        HashMap<String, User> userHashMap = new HashMap<String, User>();
        userHashMap.put("user1", CreateUser("user1"));
        userHashMap.put("user2", CreateUser("user2"));
        return userHashMap;
    }
    public HashMap<String, Schema> CreateSchema()
    {
        HashMap<String, Schema> schemaHashMap = new HashMap<String, Schema>();
        schemaHashMap.put("schema1", CreateSchema("schema1"));
        schemaHashMap.put("schema2", CreateSchema("schema2"));
        return schemaHashMap;
    }
    public HashMap<String, Site> CreateSite()
    {
        HashMap<String, Site> siteHashMap = new HashMap<String, Site>();
        siteHashMap.put("site1", CreateSite("site1"));
        siteHashMap.put("site2", CreateSite("site2"));
        return siteHashMap;
    }
    public User CreateUser(String name)
    {
        User user = new User();
        user.setUsername(name);
        user.setUid(1);
        HashMap<String, Privilege> tableMap = new HashMap<String, Privilege>();
        tableMap.put("table test", CreatePrivilege(name));
        HashMap<String, Privilege> schemaMap = new HashMap<String, Privilege>();
        schemaMap.put("schema test", CreatePrivilege(name));
        user.setTableMap(tableMap);
        user.setSchemaMap(schemaMap);
        return user;
    }
    public Privilege CreatePrivilege(String name)
    {
        Privilege privilege = new Privilege();
        privilege.setUid(1);
        privilege.setUse(GddUtil.privilegeOWNER);
        privilege.setUsername(name);
        return privilege;
    }
    public Site CreateSite(String name)
    {
        Site site = new Site();
        site.setId(1);
        site.setIp("10.77.40.31");
        site.setPort(2379);
        site.setName(name);
        site.setLeader(1);
        return site;
    }
    public Schema CreateSchema(String name)
    {
        Schema schema = new Schema();
        schema.setId(1);
        schema.setName(name);
        List<Privilege> userList = new ArrayList<Privilege>();
        userList.add(CreatePrivilege("use1"));
        userList.add(CreatePrivilege("user2"));
        schema.setUserList(userList);
        List<Table> tableList = new ArrayList<Table>();
        tableList.add(CreateTable("test1"));
        schema.setTableList(tableList);
        return schema;
    }
    public Table CreateTable(String name)
    {
        Table table = new Table();
        table.setTablename(name);
        table.setId(1);
        table.setIsFragment(1);
        List<Privilege> userList = new ArrayList<Privilege>();
        userList.add(CreatePrivilege("user1"));
        userList.add(CreatePrivilege("user2"));
        table.setPrivilegeList(userList);
        HashMap<String, Statics> staticMap = new HashMap<String, Statics>();
        staticMap.put("stuno", CreateStatics());
        table.setStaticsMap(staticMap);
        HashMap<String, Fragment> fragmentHashMap = new HashMap<String, Fragment>();
        fragmentHashMap.put("frag1", CreateFragment("frag1"));
        table.setFragment(fragmentHashMap);
        HashMap<String, Column> columnHashMap = new HashMap<String, Column>();
        columnHashMap.put("stuno", CreateColumn("stuno"));
        table.setColumns(columnHashMap);
        return table;
    }
    public Table CreateTable(String name, boolean isFragment)
    {
        Table table = new Table();
        table.setTablename(name);
        table.setId(1);
        table.setIsFragment(0);
        /*List<Privilege> userList = new ArrayList<Privilege>();
        userList.add(CreatePrivilege("user1"));
        userList.add(CreatePrivilege("user2"));
        table.setPrivilegeList(userList);*/
        HashMap<String, Statics> staticMap = new HashMap<String, Statics>();
        staticMap.put("stuno", CreateStatics());
        table.setStaticsMap(staticMap);
        /*HashMap<String, Fragment> fragmentHashMap = new HashMap<String, Fragment>();
        fragmentHashMap.put("frag1",CreateFragment("frag1"));
        table.setFragment(fragmentHashMap);*/
        HashMap<String, Column> columnHashMap = new HashMap<String, Column>();
        columnHashMap.put("stuno", CreateColumn("stuno"));
        table.setColumns(columnHashMap);
        return table;
    }
    public Statics CreateStatics()
    {
        Statics statics = new Statics();
        statics.setColumnName("stuno");
        statics.setMax("100");
        statics.setMin("10");
        statics.setMean("30");
        statics.setMode("50");
        HashMap<String, Integer> staticMap = new HashMap<String, Integer>();
        staticMap.put("100", 2);
        statics.setStaticList(staticMap);
        statics.setMedian("40");
        return statics;
    }
    public Fragment CreateFragment(String name)
    {
        Fragment fragment = new Fragment();
        fragment.setSiteName("site1");
        //fragment.setStatics(CreateStatics());
        fragment.setFragmentType(GddUtil.fragmentVERTICAL);
        fragment.setFragmentName(name);
        List<Table> tableList = new ArrayList<Table>();
        tableList.add(CreateTable("table1", false));
        fragment.setTableList(tableList);
        List<Condition> conditionList = new ArrayList<Condition>();
        conditionList.add(CreateCondition());
        fragment.setCondition(conditionList);
        return fragment;
    }
    public Condition CreateCondition()
    {
        Condition condition = new Condition();
        condition.setColumnName("stuno");
        condition.setCompareType(GddUtil.compareEQUAL);
        condition.setDataType(GddUtil.datatypeINT);
        condition.setValue("20");
        return condition;
    }
    public Column CreateColumn(String name)
    {
        Column column = new Column();
        column.setLen(4);
        column.setKey(1);
        column.setIndex(GddUtil.indexHASHINDEX);
        column.setId(1);
        column.setDataType(GddUtil.datatypeINT);
        column.setColumnName(name);
        return column;
    }
}
