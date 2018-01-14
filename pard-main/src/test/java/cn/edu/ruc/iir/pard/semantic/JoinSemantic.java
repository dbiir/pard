package cn.edu.ruc.iir.pard.semantic;

import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan2;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import org.testng.annotations.Test;

public class JoinSemantic
{
    @Test
    public void testSemantic()
    {
        UsePlan.setCurrentSchema("book");
        String stmt = "select Book.title,Book.copies,Publisher.name,Publisher.nation from Book,Publisher where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies > 1000";
        SqlParser parser = new SqlParser();
        Statement stmts = parser.createStatement(stmt);
        try {
            QueryPlan plan = new QueryPlan2(stmts);
        }
        catch (SemanticException e) {
            System.out.println(e.getSemanticErrorMessage());
            e.printStackTrace();
        }
    }
    @Test
    public void testJoin()
    {
        UsePlan.setCurrentSchema("book");
        SqlParser parser = new SqlParser();
        Statement stmt = parser.createStatement("select customer.name, orders.quantity, book.title from customer,orders,book where customer.id=orders.id and book.id=orders.book_id and customer.rank=1 and book.copies>5000");
        plan(stmt);
    }
    public QueryPlan plan(Statement stmt)
    {
        QueryPlan plan = new QueryPlan2(stmt);
        //plan.afterExecution(true);
        return plan;
    }
}
