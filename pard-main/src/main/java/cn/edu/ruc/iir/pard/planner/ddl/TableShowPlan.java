package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.commons.exception.ErrorMessage;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.ShowTables;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public class TableShowPlan
        extends Plan
{
    private String schema;

    public TableShowPlan(Statement stmt)
    {
        super(stmt);
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        Statement statement = getStatment();
        if (!(statement instanceof ShowTables)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.ParseError, "Show Tables Statement");
        }
        ShowTables showTables = (ShowTables) statement;
        Identifier schemaName = showTables.getSchema();
        if (schemaName != null) {
            schema = schemaName.getValue();
        }
        else {
            Schema schemaObj = UsePlan.getCurrentSchema();
            if (schemaObj != null) {
                schema = schemaObj.getName();
            }
        }
        if (schema == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SchemaNotSpecified);
        }
        SchemaDao schemaDao = new SchemaDao();
        Schema schemaObj = schemaDao.loadByName(schema);
        if (schemaObj == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SchemaNotExsits, schemaName);
        }
        return ErrorMessage.getOKMessage();
    }

    public String getSchema()
    {
        return schema;
    }
}
