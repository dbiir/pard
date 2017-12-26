package cn.edu.ruc.iir.pard.etcd.dao;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * pard:
 * TableDao
 * provide interface for Table's CURD.
 *
 * @author hagen
 */
public class TableDao
        extends GDDDao
{
    private SchemaDao schemaDao = null;
    private Schema schema = null;
    Map<String, Table> tmap = new HashMap<String, Table>();
    Map<String, Integer> tpos = new HashMap<String, Integer>();
    public TableDao(String schemaName)
    {
        schemaDao = new SchemaDao();
        schema = schemaDao.loadByName(schemaName);
        if (schema == null) {
            throw new NullPointerException("Schema with name " + schemaName + " not found.");
        }
//        for (Table table : schema.getTableList()) {
//            tmap.put(table.getTablename(), table);
//        }
        parseList2Map();
    }
    protected void parseList2Map()
    {
        for (int i = 0; i < schema.getTableList().size(); i++) {
            Table t = schema.getTableList().get(i);
            tmap.put(t.getTablename(), t);
            tpos.put(t.getTablename(), i);
        }
    }
    protected void parseMap2List()
    {
        List<Table> list = schema.getTableList();
        list.clear();
        for (String key : tmap.keySet()) {
            Table t = tmap.get(key);
            if (t != null) {
                list.add(t);
            }
        }
    }
    public Table loadByName(String name)
    {
        return tmap.get(name);
    }
    public boolean update(Table table)
    {
        Integer i = tpos.get(table.getTablename());
        if (i == null) {
            return false;
        }
        if (i >= schema.getTableList().size()) {
            // not reach
            return false;
        }
        tmap.put(table.getTablename(), table);
        schema.getTableList().set(i, table);
        return schemaDao.update(schema);
    }
    public boolean add(Table table, boolean check)
    {
        table.setId(schema.nextTableId());
        if (check) {
            if (tmap.get(table.getTablename()) != null) {
                return false;
            }
        }
        schema.getTableList().add(table);
        tmap.put(table.getTablename(), table);
        tpos.put(table.getTablename(), schema.getTableList().size() - 1);
        return schemaDao.update(schema);
    }
    public boolean drop(String name)
    {
        Table t = tmap.get(name);
        if (t == null) {
            return false;
        }
        tmap.put(name, null);
        tpos.put(name, null);
        parseMap2List();
        return schemaDao.update(schema);
    }
    public boolean dropAll()
    {
        tmap.clear();
        tpos.clear();
        schema.getTableList().clear();
        return schemaDao.update(schema);
    }
}
