package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.QualifiedName;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * pard
 *
 * @author guodong
 */
public class TableDropPlan
        extends TablePlan
{
    private String schemaName;
    private String tableName;
    private boolean isExists;
    private Map<String, String> distributionHints;

    public TableDropPlan(Statement stmt)
    {
        super(stmt);
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        this.distributionHints = new HashMap<>();
        Statement statement = getStatment();
        if (!(statement instanceof DropTable)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.ParseError, "Drop Table Statement");
        }
        DropTable dropTableStmt = (DropTable) statement;
        QualifiedName name = dropTableStmt.getTableName();

        if (name.getPrefix().isPresent()) {
            schemaName = name.getPrefix().get().toString();
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

        // add all sites
        Map<String, Fragment> fragmentMap = t.getFragment();
        for (String f : fragmentMap.keySet()) {
            Fragment fragment = fragmentMap.get(f);
            distributionHints.put(fragment.getSiteName(), "");
        }
//        SiteDao siteDao = new SiteDao();
//        for (String site : siteDao.listNodes().keySet()) {
//            distributionHints.put(site, "");
//        }

        return ErrorMessage.getOKMessage();
    }

    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        SchemaDao schemaDao = new SchemaDao();
        if (!alreadyDone) {
            Schema schema = schemaDao.loadByName(schemaName);
            if (schema != null) {
                List<Table> tables = schema.getTableList();
                int index = 0;
                for (int i = 0; i < tables.size(); i++) {
                    Table table = tables.get(i);
                    if (table.getTablename().equalsIgnoreCase(tableName)) {
                        index = i;
                        break;
                    }
                }
                schema.getTableList().remove(index);
                schemaDao.update(schema);
            }
        }
        return true;
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

    public Map<String, String> getDistributionHints()
    {
        return distributionHints;
    }
}
