package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public class TableShowPlan
        extends Plan
{
    private String schema = null;

    public TableShowPlan(Statement stmt)
    {
        super(stmt);
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        Statement statement = getStatment();
        if (!(statement instanceof DropTable)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.ParseError, "Drop Table Statement");
        }
        return null;
    }
}
