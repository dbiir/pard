package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.sql.tree.ColumnDefinition;
import cn.edu.ruc.iir.pard.sql.tree.CreateTable;
import cn.edu.ruc.iir.pard.sql.tree.QualifiedName;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.sql.tree.TableElement;
import cn.edu.ruc.iir.pard.sql.tree.TableHPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableVPartitioner;

import java.util.ArrayList;
import java.util.List;

public class TableCreationPlan
        extends TablePlan
{
    private CreateTable stmt = null;
    private String tableName = null;
    private String schemaName = null;
    private boolean isNotExists = false;
    @Override
    public boolean beforeExecution()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        // TODO Auto-generated method stub
        return false;
    }
/**
 * 目前只考虑垂直分区和水平分区，不考虑混合分区
 *
 * */
    @Override
    public ErrorMessage semanticAnalysis()
    {
        Statement statement = this.getStatment();
        if (!(statement instanceof CreateTable)) {
            return ErrorMessage.throwMessage(ErrCode.ParseError, "Create Table Statement");
        }
        stmt = (CreateTable) statement;
        TableHPartitioner hp = stmt.getHorizontalPartition().isPresent() ? stmt.getHorizontalPartition().get() : null;
        List<TableVPartitioner> vp = stmt.getVerticalPartitions();
        QualifiedName name = stmt.getName();
        Schema schema = UsePlan.getCurrentSchema();
        if (schema != null) {
            schemaName = schema.getName();
        }
        if (name.getPrefix().isPresent()) {
            schemaName = name.getPrefix().get().toString();
        }
        if (schemaName == null) {
            return ErrorMessage.throwMessage(ErrCode.SchemaNotSpecified);
        }
        SchemaDao dao = new SchemaDao();
        if (schema == null) {
            schema = dao.loadByName(schemaName);
        }
        if (schema == null) {
            return ErrorMessage.throwMessage(ErrCode.SchemaNotExsits, schemaName);
        }
        tableName = name.getSuffix();
        isNotExists = stmt.isNotExists();
        TableDao tdao = new TableDao(schemaName);
        Table t = tdao.loadByName(tableName);
        if (t != null) {
            if (isNotExists) {
                alreadyDone = true;
                return ErrorMessage.getOKMessage();
            }
            else {
                return ErrorMessage.throwMessage(ErrCode.TableExists, tableName, schemaName);
            }
        }
        List<ColumnDefinition> list = new ArrayList<ColumnDefinition>();
        for (TableVPartitioner v : vp) {
            for (TableElement e : v.getElements()) {
                if (e instanceof ColumnDefinition) {
                    list.add((ColumnDefinition) e);
                }
                else {
                    return ErrorMessage.throwMessage(ErrCode.NotaColumnDefinition, e.toString());
                }
            }
        }
        for (ColumnDefinition cd : list) {
            //TODO: check each column's name and type.
        }
        return null;
    }
}
