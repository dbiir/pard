package cn.edu.ruc.iir.pard.semantic;

import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Node;
import cn.edu.ruc.iir.pard.sql.tree.OrderBy;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.QueryBody;
import cn.edu.ruc.iir.pard.sql.tree.SortItem;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.List;

public class SemanticAnalysisTest
{
    private SqlParser parser = new SqlParser();
    @Test
    public void analysis()
    {
        String sql =
                "SELECT id, name FROM test0 JOIN test1 ON test0.id=test1.id where test0.id>10 order by id,name";
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
}
