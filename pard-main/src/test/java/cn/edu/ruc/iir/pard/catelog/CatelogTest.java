package cn.edu.ruc.iir.pard.catelog;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import org.testng.annotations.Test;

public class CatelogTest
{
    @Test
    public void createSchema()
    {
        SchemaDao schemaDao = new SchemaDao();
        Schema schema = new Schema();
        schema.setName("testSchema");
        schemaDao.add(schema, true);
       // schema = schemaDao.loadByName("testSchema");
        TableDao tableDao = new TableDao("testSchema");
    }
}
