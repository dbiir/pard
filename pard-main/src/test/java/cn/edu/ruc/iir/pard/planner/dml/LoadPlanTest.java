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
        String str = "load \"/home/hagen/emp.tsv\" into pardtest.emp";
        Statement stmt = parser.createStatement(str);
        LoadPlan plan = new LoadPlan(stmt);
    }
}
