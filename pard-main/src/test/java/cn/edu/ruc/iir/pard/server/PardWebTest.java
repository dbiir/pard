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
        /*
        Statement stmt = parser.createStatement("select Book.title,Book.copies,Publisher.name,Publisher.nation from Book,Publisher where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies > 1000");
        plan(stmt);
        stmt = parser.createStatement("select customer.name, orders.quantity, book.title from customer,orders,book where customer.id=orders.customer_id and book.id=orders.book_id and customer.rank=1 and book.copies>5000");
        plan(stmt);
        stmt = parser.createStatement("select * from customer");
        plan(stmt);
        stmt = parser.createStatement("select * from publisher");
        plan(stmt);*/
        Statement stmt = parser.createStatement("select customer.name, orders.quantity from customer, orders where customer.id=orders.customer_id");
        plan(stmt);
        stmt = parser.createStatement("select customer.name, customer.rank, orders.quantity from customer, orders where customer.id=orders.customer_id and customer.rank=1");
        plan(stmt);
        stmt = parser.createStatement("select customer.name, orders.quantity, book.title from customer, orders, book where customer.id=orders.customer_id and book.id=orders.book_id and customer.rank=1 and book.copies>5000");
        plan(stmt);
        stmt = parser.createStatement("select customer.name, book.title, publisher.name, orders.quantity from customer, book, publisher, orders where customer.id=orders.customer_id and book.id=orders.book_id and book.publisher_id=publisher.id and book.id>220000 and publisher.nation='USA' and orders.quantity>1");
        plan(stmt);
        stmt = parser.createStatement("select customer.name, book.title, publisher.name, orders.quantity from customer, book, publisher, orders where customer.id=orders.customer_id and book.id=orders.book_id and book.publisher_id=publisher.id and customer.id>308000 and book.copies>100 and orders.quantity>1 and publisher.nation='PRC'");
        plan(stmt);
        System.out.println(PardServlet.planList.size());
        PardWebServer.main(new String[0]);
    }
    public QueryPlan plan(Statement stmt)
    {
        try {
            QueryPlan plan = new QueryPlan2(stmt);
            plan.afterExecution(true);
            return plan;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
