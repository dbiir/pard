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
                "create table emp\n"
                + "("
                + "eno char(20) primary key,"
                + "ename char(100),"
                + "title char(100)"
                + ")"
                + "partition by  range"
                + "("
                + "p0 eno < 'E1000' AND title < 'N' on node1,"
                + "p1 eno < 'E1000' AND title >='N' on node2,"
                + "p2 eno >= 'E1000' AND title < 'N' on node3,"
                + "p3 eno >= 'E1000' AND title < 'N' on node4"
                + ")";
        Statement stmt = parser.createStatement(sql);
        System.out.println(stmt.toString());
        String sql2 = "use testSchema";
        Statement useStmt = parser.createStatement(sql2);
        UsePlan plan = new UsePlan(useStmt);
        //plan.setStatment(useStmt);
        ErrorMessage msg = plan.semanticAnalysis();
        System.out.println(msg.toString());

        TableCreationPlan tcp = new TableCreationPlan(stmt);
        //tcp.setStatment(stmt);
        msg = tcp.semanticAnalysis();
        System.out.println(msg);
        tcp.afterExecution(true);
        String schemaName = "testSchema";
        TableDao tdao = new TableDao(schemaName);
        SchemaDao schemaDao = new SchemaDao();
        Table t = schemaDao.loadByName(schemaName).getTableList().get(0);
        t = tdao.loadByName("emp");
        System.out.println(JSONObject.fromObject(t).toString(1));
        tdao.dropAll();
    }
    @Test
    public void insertInto()
    {
        String sql = "insert into emp (eno, ename, title) values ('E0001', 'J. Doe', 'Elect. Eng.')";
        Statement stmt = parser.createStatement(sql);
        System.out.println(stmt.getClass());
    }
    @Test
    public void select()
    {
        String sql = "select * from EMP where eno < 'E0010' and eno > 'E0000'";
        Statement stmt = parser.createStatement(sql);
        System.out.println(stmt);
    }
}
