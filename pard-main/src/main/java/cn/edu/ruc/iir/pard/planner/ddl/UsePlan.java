package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.planner.GDDPlan;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.sql.tree.Use;

public class UsePlan
        extends GDDPlan
{
    public UsePlan(Statement stmt)
    {
        super(stmt);
    }
    protected boolean alreadyDone = false;
    private String schemaName = null;
    private static Use stmt = null;
    private static SchemaDao dao = new SchemaDao();
    private static ThreadLocal<String> currentSchema = new ThreadLocal<String>();
    public static Schema getCurrentSchema()
    {
        String name = currentSchema.get();
        if (name != null) {
            return dao.loadByName(name);
        }
        return null;
    }
    public static void clearCurrentSchema()
    {
        currentSchema.set(null);
    }
    public static Schema setCurrentSchema(String name)
    {
        Schema schema = dao.loadByName(name);
        if (schema != null) {
            currentSchema.set(name);
        }
        return schema;
    }
    @Override
    public ErrorMessage semanticAnalysis()
    {
        Statement statement = this.getStatment();
        if (!(statement instanceof Use)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.ParseError, "Create Schema Statement");
        }
        stmt = (Use) statement;
        schemaName = stmt.getSchema().toString();
        Schema schema = dao.loadByName(schemaName);
        if (schema == null) {
            return ErrorMessage.throwMessage(ErrCode.SchemaNotExsits, schemaName);
        }
        currentSchema.set(schemaName);
        alreadyDone = true;
        return ErrorMessage.getOKMessage();
    }
    public boolean isAlreadyDone()
    {
        return alreadyDone;
    }
    @Override
    public boolean beforeExecution()
    {
        return true;
    }
    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        return true;
    }
}
