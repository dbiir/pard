package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan2;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.web.PardServlet;
import org.testng.annotations.Test;

public class PardWebTest
{
    @Test
    public void test()
    {
        SqlParser parser = new SqlParser();
        UsePlan.setCurrentSchema("book");
        Statement stmt = parser.createStatement("select Book.title,Book.copies,Publisher.name,Publisher.nation from Book,Publisher where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies > 1000");
        plan(stmt);
        stmt = parser.createStatement("select Book.title,Book.copies,Publisher.name,Publisher.nation from Book,Publisher where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies > 1000");
        plan(stmt);
        System.out.println(PardServlet.planList.size());
        PardWebServer.main(new String[0]);
    }
    public QueryPlan plan(Statement stmt)
    {
        QueryPlan plan = new QueryPlan2(stmt);
        plan.afterExecution(true);
        return plan;
    }
}
