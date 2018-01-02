package cn.edu.ruc.iir.pard.etcd.dao;

import cn.edu.ruc.iir.pard.catalog.GDD;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.etcd.WatchThread;

import java.util.Map;
/**
 * pard
 * SchemaDao
 * provide interface for Schema's CURD
 *
 * @author hagen
 * */
public class SchemaDao
        extends GDDDao
{
    private WatchThread watchThread;

    public SchemaDao()
    {
        //watchThread = new WatchThread("schema");
        //watchThread.start();
    }

    public Schema loadByName(String name)
    {
        Schema schema = null;
        GDD gdd = load();
        schema = gdd.getSchemaMap().get(name);
        return schema;
    }

    public boolean update(Schema schema)
    {
        GDD gdd = load();
        Map<String, Schema> schemaMap = gdd.getSchemaMap();
        schemaMap.put(schema.getName(), schema);
        return persistGDD(gdd);
    }

    public boolean add(Schema schema, boolean check)
    {
        GDD gdd = load();
        Map<String, Schema> schemaMap = gdd.getSchemaMap();
        if (check) {
            if (schemaMap.get(schema.getName()) != null) {
                return false;
            }
        }
        schema.setId(gdd.nextSchemaId());
        schemaMap.put(schema.getName(), schema);
        return persistGDD(gdd);
    }

    public boolean drop(String name)
    {
        GDD gdd = load();
        Map<String, Schema> schemaMap = gdd.getSchemaMap();
        schemaMap.put(name, null);
        return persistGDD(gdd);
    }

    public boolean dropAll()
    {
        GDD gdd = load();
        Map<String, Schema> schemaMap = gdd.getSchemaMap();
        schemaMap.clear();
        return persistGDD(gdd);
    }
}
