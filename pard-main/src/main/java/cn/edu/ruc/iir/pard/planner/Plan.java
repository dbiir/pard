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
    private Statement statement;
    private String jobId;
    private ErrorMessage msg;
    public Plan(Statement stmt)
    {
        statement = stmt;
        msg = this.semanticAnalysis();
        if (msg.getErrcode() < 0) {
            System.err.println(msg.toString());
            throw new SemanticException(msg);
        }
    }

    public ErrorMessage getMsg()
    {
        return msg;
    }

    public void setMsg(ErrorMessage msg)
    {
        this.msg = msg;
    }

    public Statement getStatment()
    {
        return statement;
    }

    public void setStatment(Statement statment)
    {
        this.statement = statment;
    }

    public String getJobId()
    {
        return jobId;
    }

    public void setJobId(String jobId)
    {
        this.jobId = jobId;
    }

    public abstract ErrorMessage semanticAnalysis();

    public boolean afterExecution(boolean executeSuccess)
    {
        return true;
    }
}
