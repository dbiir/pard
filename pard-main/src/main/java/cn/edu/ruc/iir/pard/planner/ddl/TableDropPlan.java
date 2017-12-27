package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public class TableDropPlan
        extends SchemaDropPlan
{
    public TableDropPlan(Statement stmt)
    {
        super(stmt);
    }
}
