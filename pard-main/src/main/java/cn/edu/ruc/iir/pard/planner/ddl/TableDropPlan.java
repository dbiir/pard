package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.QualifiedName;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public class TableDropPlan
        extends TablePlan
{
    private String schemaName = null;
    private String tableName;
    private boolean isExists;

    public TableDropPlan(Statement stmt)
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
        DropTable dropTableStmt = (DropTable) statement;
        QualifiedName name = dropTableStmt.getTableName();

        if (name.getPrefix().isPresent()) {
            schemaName = name.getPrefix().toString();
        }
        else {
            Schema schema = UsePlan.getCurrentSchema();
            if (schema != null) {
                schemaName = schema.getName();
            }
        }
        if (schemaName == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SchemaNotSpecified);
        }
        tableName = name.getSuffix();
        isExists = dropTableStmt.isExists();
        TableDao tableDao = new TableDao(schemaName);
        Table t = tableDao.loadByName(tableName);
        if (t == null) {
            if (isExists) {
                alreadyDone = true;
                return ErrorMessage.getOKMessage();
            }
            else {
                return ErrorMessage.throwMessage(ErrorMessage.ErrCode.TableNotExists, tableName, schemaName);
            }
        }

        return ErrorMessage.getOKMessage();
    }

    @Override
    public boolean isAlreadyDone()
    {
        return alreadyDone;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public boolean isExists()
    {
        return isExists;
    }
}
