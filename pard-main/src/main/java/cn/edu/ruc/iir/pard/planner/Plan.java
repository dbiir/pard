package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.semantic.SemanticException;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Plan
{
    private Statement statment;
    /**
     *
     * @throws SemanticException
     *
     * @author hagen
     * */
    public Plan(Statement stmt)
    {
        statment = stmt;
//        ErrorMessage msg = semanticAnalysis();
//        if (msg.getErrcode() < 0) {
//            System.err.println(msg.toString());
//            throw new SemanticException(msg);
//        }
    }
    public Statement getStatment()
    {
        return statment;
    }

    public void setStatment(Statement statment)
    {
        this.statment = statment;
    }

    public abstract ErrorMessage semanticAnalysis();

    public boolean afterExecution(boolean executeSuccess)
    {
        return true;
    }
}
