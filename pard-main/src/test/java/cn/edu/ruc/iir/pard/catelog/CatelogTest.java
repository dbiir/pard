package cn.edu.ruc.iir.pard.catelog;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.PardPlanner;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.DeletePlan;
import cn.edu.ruc.iir.pard.planner.dml.InsertPlan;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Row;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

public class CatelogTest
{
    private SqlParser parser = new SqlParser();
    @Test
    public void createSchema()
    {
        SchemaDao schemaDao = new SchemaDao();
        for (String schemaName : schemaDao.listAll()) {
            TableDao tableDao = new TableDao(schemaName);
            System.out.println("Schema:" + schemaName);
            for (Table table : tableDao.getTableList()) {
                System.out.println("\tTable: " + table.getTablename());
                for (Column col : table.getColumns().values()) {
                    System.out.print(col.getColumnName() + "\t");
                }
                System.out.println();
            }
        }
        //schema.setName("testSchema");
        //schemaDao.add(schema, true);
       // schema = schemaDao.loadByName("testSchema");
        //TableDao tableDao = new TableDao("testSchema");
        //tableDao.dropAll();
    }
    @Test
    public void viewTable()
    {
        String schemaName = "pardtest";
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
    public void viewSite()
    {
        SiteDao siteDao = new SiteDao();
        siteDao.listNodes().keySet().forEach(System.out::println);
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
                + "p0 eno < 'E1000' AND title < 'N' on pard0,"
                + "p1 eno < 'E1000' AND title >='N' on pard1,"
                + "p2 eno >= 'E1000' AND title < 'N' on pard2,"
                + "p3 eno >= 'E1000' AND title >= 'N' on pard3"
                + ")";
        Statement stmt = parser.createStatement(sql);
        System.out.println(stmt.toString());
        String sql2 = "use pardtest";
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
        String schemaName = "pardtest";
        TableDao tdao = new TableDao(schemaName);
        SchemaDao schemaDao = new SchemaDao();
        Table t = schemaDao.loadByName(schemaName).getTableList().get(0);
        t = tdao.loadByName("emp");
        System.out.println(JSONObject.fromObject(t).toString(1));
        //tdao.dropAll();
    }
    @Test
    public void insertInto()
    {
        String sql2 = "use testSchema";
        Statement useStmt = parser.createStatement(sql2);
        UsePlan plan = new UsePlan(useStmt);
        String sql = "insert into emp (eno, ename, title) values ('E0001', 'J. Doe', 'Elect. Eng.'),('E1002', 'J. Doe', 'Elect. Eng.')";
        Statement stmt = parser.createStatement(sql);
        System.out.println(stmt);
        InsertPlan iplan = new InsertPlan(stmt);
        iplan.semanticAnalysis();
        Map<String, List<Row>> map = iplan.getDistributionHints();
        System.out.print("node\t");
        /*
        List<Column> clist = iplan.getColListMap();
        for (Column col : clist) {
            System.out.print(col.getColumnName() + "\t");
        }*/
        System.out.println();
        for (String key : map.keySet()) {
            List<Row> row = map.get(key);
            for (Row r : row) {
                System.out.print(key + "\t");
                for (Expression e : r.getItems()) {
                    System.out.print(e + "\t");
                }
                System.out.println("");
            }
        }
    }
    @Test
    public void deleteInto()
    {
        String sql2 = "use pardtest";
        Statement useStmt = parser.createStatement(sql2);
        System.out.println("sql2:" + useStmt);
        UsePlan plan = new UsePlan(useStmt);
        ErrorMessage msg = plan.semanticAnalysis();
        System.out.println(msg);
        String sql = "delete from emp where eno < 'E0010'";
        Statement stmt = parser.createStatement(sql);
        System.out.println("sql:" + sql + "," + stmt);
        DeletePlan dplan = new DeletePlan(stmt);
//        msg = dplan.semanticAnalysis();
//        System.out.println(msg);
        Map<String, Expr> map = dplan.getDistributionHints();
        for (String key : map.keySet()) {
            System.out.println("fragment:" + key + ", expr:" + map.get(key).toString());
        }
        System.out.print("node\t");
    }
    @Test
    public void select()
    {
        String sql = "select * from EMP where eno < 'E0010' and eno > 'E0000'";
        Statement stmt = parser.createStatement(sql);
        PardPlanner planner = new PardPlanner();
        Plan plan = planner.plan(stmt);
        //System.out.println(stmt);
    }
}
