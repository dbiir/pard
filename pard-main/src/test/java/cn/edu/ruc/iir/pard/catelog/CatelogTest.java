package cn.edu.ruc.iir.pard.catelog;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.testng.annotations.Test;

public class CatelogTest
{
    private SqlParser parser = new SqlParser();
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
    @Test
    public void viewTable()
    {
        String schemaName = "testSchema";
        SchemaDao schemaDao = new SchemaDao();
        Schema schema = schemaDao.loadByName(schemaName);
        TableDao tdao = new TableDao(schemaName);
        if (schema != null) {
            System.out.println(JSONArray.fromObject(schema.getTableList()).toString(1));
            System.out.println(schema.getTableList().get(0).getFragment().size());
        }
        tdao.dropAll();
    }
    @Test
    public void createTable()
    {
        String sql =
                "CREATE TABLE if not exists orders_range\n" +
                "(\n" +
                "id INT PRIMARY KEY,\n" +
                "name VARCHAR(30),\n" +
                "order_date DATE\n" +
                ") PARTITION BY RANGE\n" +
                "(\n" +
                "p0 id < 5 AND order_date >= '2017-01-01' ON node1,\n" +
                "p1 id < 10 ON node2,\n" +
                "p2 id < 15 ON node3\n" +
                ")";
        Statement stmt = parser.createStatement(sql);
        String sql2 = "use testSchema";
        Statement useStmt = parser.createStatement(sql2);
        UsePlan plan = new UsePlan();
        plan.setStatment(useStmt);
        ErrorMessage msg = plan.semanticAnalysis();
        System.out.println(msg.toString());

        TableCreationPlan tcp = new TableCreationPlan();
        tcp.setStatment(stmt);
        msg = tcp.semanticAnalysis();
        System.out.println(msg);
        tcp.afterExecution(true);
        String schemaName = "testSchema";
        TableDao tdao = new TableDao(schemaName);
        SchemaDao schemaDao = new SchemaDao();
        Table t = schemaDao.loadByName(schemaName).getTableList().get(0);
        t = tdao.loadByName("orders_range");
        System.out.println(JSONObject.fromObject(t).toString(1));
        tdao.dropAll();
    }
}
