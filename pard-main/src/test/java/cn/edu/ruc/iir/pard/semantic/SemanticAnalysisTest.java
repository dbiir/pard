package cn.edu.ruc.iir.pard.semantic;

import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import org.testng.annotations.Test;

public class SemanticAnalysisTest
{
    private SqlParser parser = new SqlParser();
    @Test
    public void analysis()
    {
        String sql =
                "SELECT id, name FROM test0 JOIN test1 ON test0.id=test1.id where test0.id>10";
        Statement statement = parser.createStatement(sql);
        if (statement instanceof Query) {
            Query q = (Query) statement;
            System.out.println("it is a query");
        }
        System.out.println(statement.toString());
    }
}
