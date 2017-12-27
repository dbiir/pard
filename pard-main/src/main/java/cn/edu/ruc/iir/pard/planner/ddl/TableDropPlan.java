package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public class TableDropPlan
        extends TablePlan
{
    public TableDropPlan(Statement stmt)
    {
        super(stmt);
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        return null;
    }
}
