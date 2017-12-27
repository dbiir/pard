package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.sql.tree.CreateSchema;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

public class SchemaCreationPlan
        extends SchemaPlan
{
    public SchemaCreationPlan(Statement stmt)
    {
        super(stmt);
    }
    private String schemaName = null;
    private Schema schema = null;
    private boolean isNotExists = false;
    private CreateSchema stmt = null;
    public String getSchemaName()
    {
        return schemaName;
    }

    public Schema getSchema()
    {
        return schema;
    }

    public boolean isNotExists()
    {
        return isNotExists;
    }

    public CreateSchema getStmt()
    {
        return stmt;
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        Statement statement = this.getStatment();
        if (!(statement instanceof CreateSchema)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.ParseError, "Create Schema Statement");
        }
        stmt = (CreateSchema) statement;
        schemaName = stmt.getSchemaName().toString();
        isNotExists = stmt.isNotExists();
        SchemaDao schemaDao = new SchemaDao();
        schema = schemaDao.loadByName(schemaName);
        if (schema != null) {
            if (isNotExists) {
                // has 'if not exsits'
                //OK
                alreadyDone = true;
                return ErrorMessage.getOKMessage();
            }
            else {
                return ErrorMessage.throwMessage(ErrCode.SchemaExsits, schemaName);
            }
        }
        return ErrorMessage.getOKMessage();
    }
    @Override
    public boolean beforeExecution()
    {
        return true;
    }
    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        if (executeSuccess && !alreadyDone) {
            if (schema == null) {
                SchemaDao schemaDao = new SchemaDao();
                schema = new Schema();
                schema.setName(schemaName);
                return schemaDao.add(schema, true);
            }
        }
        return true;
    }
}
