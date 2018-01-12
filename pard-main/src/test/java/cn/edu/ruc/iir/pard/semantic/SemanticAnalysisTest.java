package cn.edu.ruc.iir.pard.semantic;

import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.executor.connector.node.AggregationNode;
import cn.edu.ruc.iir.pard.executor.connector.node.DistinctNode;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.InputNode;
import cn.edu.ruc.iir.pard.executor.connector.node.JoinNode;
import cn.edu.ruc.iir.pard.executor.connector.node.LimitNode;
import cn.edu.ruc.iir.pard.executor.connector.node.NodeHelper;
import cn.edu.ruc.iir.pard.executor.connector.node.OutputNode;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.ProjectNode;
import cn.edu.ruc.iir.pard.executor.connector.node.SortNode;
import cn.edu.ruc.iir.pard.executor.connector.node.TableScanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.UnionNode;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.PardPlanner;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Node;
import cn.edu.ruc.iir.pard.sql.tree.OrderBy;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.QueryBody;
import cn.edu.ruc.iir.pard.sql.tree.SortItem;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import org.testng.annotations.Test;

import java.util.List;

public class SemanticAnalysisTest
{
    private SqlParser parser = new SqlParser();

    @Test
    public void analysis()
    {
        String sql =
                "SELECT * FROM test0";
//                "SELECT id, name FROM test0 JOIN test1 ON test0.id=test1.id where test0.id>10 order by id,name";
        Statement statement = parser.createStatement(sql);
        if (statement instanceof Query) {
            Query q = (Query) statement;
            System.out.println("it is a query");
            System.out.println(q.toString());
            List<? extends Node> children = q.getChildren();
            System.out.println("children:");
            for (Node n : children) {
                System.out.println(n.toString());
            }
            PlanNode node = parseQuery(q);
            if (q.getOrderBy().isPresent()) {
                node = parseOrderBy(node, q.getOrderBy().get());
            }
        }
        System.out.println(statement.toString());
    }

    @Test
    public void testInsertPlan()
    {
        String sql = "INSERT INTO ewsddsds.EMP(eno, ename, title) VALUES('E0001', 'J. Doe', 'Elect. Eng.')";
        Statement stmt = parser.createStatement(sql);
        PardPlanner planner = new PardPlanner();
        Plan plan = planner.plan(stmt);
        System.out.println(plan);
    }

    private PlanNode parseOrderBy(PlanNode node, OrderBy orderBy)
    {
        List<SortItem> item = orderBy.getSortItems();
        return null;
    }
    public PlanNode parseQuery(Query query)
    {
        PlanNode p = null;
        QueryBody body = query.getQueryBody();
        return null;
    }
    public PlanNode parseQueryBody(QueryBody body)
    {
        PlanNode p = null;

        return null;
    }
    @Test
    public void testVP()
    {
        String sql2 = "use pardtest";
        TableDao tdao = new TableDao("pardtest");
        tdao.dropAll();
        Statement useStmt = parser.createStatement(sql2);
        Plan plan = new UsePlan(useStmt);
        plan.semanticAnalysis();
        String sql =
                "create table Customer"
                + "(id int primary key,name char(25)) on pard0,(id int primary key,rank int) on pard1";
        Statement statement = parser.createStatement(sql);
        TableCreationPlan ct = new TableCreationPlan(statement);
        ct.afterExecution(true);
        ErrorMessage msg = ct.semanticAnalysis();
        System.out.println(JSONObject.fromObject(ct.getTable()).toString(1));
        System.out.println(msg);
    }
    @Test
    public void testQueryPlanner()
    {
        PardPlanner planner = new PardPlanner();
        String sql2 = "use pardtest";
        Statement useStmt = parser.createStatement(sql2);
        Plan plan = new UsePlan(useStmt);
       //plan.semanticAnalysis();
        //plan.afterExecution(true);
        String sql = "SELECT * FROM emp where eno < 'E0010' and eno > 'E0000'";
        Statement statement = parser.createStatement(sql);
        //UsePlan.setCurrentSchema("pard");
        plan = planner.plan(statement);
        //System.out.println(JSON.toJSONString(plan));
        JsonConfig config = new JsonConfig();
        config.setIgnoreDefaultExcludes(false); //设置默认忽略
        config.setExcludes(new String[]{"parent", "statment"});
        if (plan instanceof QueryPlan) {
            PlanNode node = ((QueryPlan) plan).optimize();
           //System.out.println(toJSON().toString(1));
            System.out.println(node.toString());
            System.out.println(NodeHelper.copyNode(node).toString());
        }
        //ErrorMessage errorMessage = plan.semanticAnalysis();
        /*
        if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
            System.out.println("OK");
            System.out.println(((QueryPlan) plan).getPlan());
        }
        else {
            System.out.println("Plan error " + errorMessage.getErrmsg());
        }*/
    }
    public JSONObject toJSON(PlanNode node)
    {
        if (node instanceof TableScanNode) {
            TableScanNode cnode = (TableScanNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "TABLESCAN");
            obj.put("schema", cnode.getSchema());
            obj.put("table", cnode.getTable());
            obj.put("site", cnode.getSite());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof UnionNode) {
            UnionNode cnode = (UnionNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "UNION");
            JSONArray array = new JSONArray();
            for (PlanNode p : cnode.getUnionChildren()) {
                array.add(toJSON(p));
            }
            obj.put("children", array);
            return obj;
        }
        else if (node instanceof SortNode) {
            SortNode cnode = (SortNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "SORT");
            obj.put("columns", cnode.getColumns());
            obj.put("orderings", cnode.getOrderings());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof ProjectNode) {
            ProjectNode cnode = (ProjectNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "PROJECT");
            obj.put("columns", cnode.getColumns());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof OutputNode) {
            OutputNode cnode = (OutputNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "OUTPUT");
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof LimitNode) {
            LimitNode cnode = (LimitNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "LIMIT");
            obj.put("number", ((LimitNode) node).getLimitNum());
            obj.put("child", toJSON(cnode.getLeftChild()));
        }
        else if (node instanceof JoinNode) {
            JoinNode cnode = (JoinNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "JOIN");
            JSONArray array = new JSONArray();
            for (PlanNode p : cnode.getJoinChildren()) {
                array.add(toJSON(p));
            }
            obj.put("children", array);
            obj.put("joinSet", cnode.getJoinSet());
        }
        else if (node instanceof InputNode) {
            InputNode cnode = (InputNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "INPUT");
        }
        else if (node instanceof FilterNode) {
            FilterNode cnode = (FilterNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "FILTER");
            obj.put("expression", cnode.getExpression().toString());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof DistinctNode) {
            DistinctNode cnode = (DistinctNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "DISTINCT");
            obj.put("columns", cnode.getColumns());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof AggregationNode) {
            AggregationNode cnode = (AggregationNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "AggragationNode");
            return obj;
        }
        return null;
    }
}
