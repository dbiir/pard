package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import org.testng.annotations.Test;

public class LoadPlanTest
{
    @Test
    public void semanticAnalysis()
    {
        SqlParser parser = new SqlParser();
        String str = "load \"/home/hagen/下载/eval_db/customer1.tsv\" into pardtest.customer";
        Statement stmt = parser.createStatement(str);
        LoadPlan plan = new LoadPlan(stmt);
    }
    @Test
    public void semanticAnalysis2()
    {
        SqlParser parser = new SqlParser();
        String str = "insert into book.customer(id, name, rank) values(300001, 'J. Stephenson', 3)";
        Statement stmt = parser.createStatement(str);
        InsertPlan plan = new InsertPlan(stmt);
    }
}
