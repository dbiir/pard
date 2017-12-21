package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Plan
{
    private Statement statment = null;
    public Statement getStatment()
    {
        return statment;
    }

    public void setStatment(Statement statment)
    {
        this.statment = statment;
    }
    public abstract ErrorMessage semanticAnalysis();
}
