package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.DataType;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.sql.tree.ColumnDefinition;
import cn.edu.ruc.iir.pard.sql.tree.CreateTable;
import cn.edu.ruc.iir.pard.sql.tree.QualifiedName;
import cn.edu.ruc.iir.pard.sql.tree.RangePartitionElement;
import cn.edu.ruc.iir.pard.sql.tree.RangePartitionElementCondition;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.sql.tree.TableElement;
import cn.edu.ruc.iir.pard.sql.tree.TableHHashPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHListPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHRangePartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableVPartitioner;

import java.util.ArrayList;
import java.util.List;

public class TableCreationPlan
        extends TablePlan
{
    private CreateTable stmt = null;
    private String tableName = null;
    private String schemaName = null;
    private Table table = null;
    private boolean isNotExists = false;
    private TableDao tableDao = null;

    public TableCreationPlan(Statement stmt)
    {
        super(stmt);
    }

    @Override
    public boolean beforeExecution()
    {
        return true;
    }

    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        // TODO Auto-generated method stub
        if (executeSuccess) {
            if (table != null && tableDao != null) {
                return tableDao.add(table, false);
            }
            else {
                return false;
            }
        }
        else {
            return true;
        }
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
        // collect vp & hp
        TableHPartitioner hp = stmt.getHorizontalPartition().isPresent() ? stmt.getHorizontalPartition().get() : null;
        List<TableVPartitioner> vp = stmt.getVerticalPartitions();
        QualifiedName name = stmt.getName();
        //check schema
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
        // check table definition
        tableName = name.getSuffix();
        isNotExists = stmt.isNotExists();
        tableDao = new TableDao(schemaName);
        Table t = tableDao.loadByName(tableName);
        if (t != null) {
            if (isNotExists) {
                alreadyDone = true;
                return ErrorMessage.getOKMessage();
            }
            else {
                return ErrorMessage.throwMessage(ErrCode.TableExists, tableName, schemaName);
            }
        }
        // check column
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
        table = new Table();
        table.setTablename(tableName);
        for (ColumnDefinition cd : list) {
            String type = cd.getType();
            String colName = cd.getName().toString();
            //System.out.println("type:" + type + " colName: " + colName);
            DataType dt = DataType.getDataType(type);
            if (dt == null) {
                return ErrorMessage.throwMessage(ErrCode.ColumnDataTypeNotExists, colName, type);
            }
            Column column = new Column();
            column.setId(table.nextColumnId());
            column.setColumnName(colName);
            column.setDataType(dt.getType());
            column.setLen(dt.getLength());
            column.setKey(cd.isPrimary() ? 1 : 0);
            table.getColumns().put(column.getColumnName(), column);
        }

        // check partition
        if (hp != null && hp instanceof TableHRangePartitioner) {
            // do sth
            TableHRangePartitioner thp = (TableHRangePartitioner) hp;
            table.setIsFragment(1);
            return parseRangePartition(thp);
        }
        else
            if (hp != null && hp instanceof TableHHashPartitioner) {
                return ErrorMessage.throwMessage(ErrCode.HorizontalPartitionApproachNotImplement, "Hash Partition");
            }
            else
                if (hp != null && hp instanceof TableHListPartitioner) {
                    return ErrorMessage.throwMessage(ErrCode.HorizontalPartitionApproachNotImplement, "List Partition");
                }
                else
                    if (hp != null) {
                        return ErrorMessage.throwMessage(ErrCode.UnknownHorizontalPartition, hp.toString());
                    }
                    else
                        if (hp == null && vp.size() > 1) {
                            return ErrorMessage.throwMessage(ErrCode.VerticalPartitionNotImplement);
                        }
                        else {
                            // 单垂直分片
                            return ErrorMessage.throwMessage(ErrCode.VerticalPartitionNotImplement);
                        }
    }

    private ErrorMessage parseRangePartition(TableHRangePartitioner thp)
    {
        SiteDao siteDao = new SiteDao();
        for (RangePartitionElement elem : thp.getElements()) {
            String nodeId = elem.getNodeId();
            List<RangePartitionElementCondition> conditions = elem.getConditions();
            String partitionName = elem.getPartitionName().toString();
            Site site = siteDao.loadByName(nodeId);
            if (site == null) {
                return ErrorMessage.throwMessage(ErrCode.SiteNotExist, nodeId);
            }
            Fragment frag = new Fragment();
            frag.setFragmentName(partitionName);
            frag.setFragmentType(GddUtil.fragementHORIZONTIAL);
            frag.setSiteName(site.getName());
            frag.setSubTable(null);
            for (RangePartitionElementCondition cond : conditions) {
                String columnName = cond.getPartitionColumn().getValue();
                Column col = table.getColumns().get(columnName);
                if (col == null) {
                    return ErrorMessage.throwMessage(ErrCode.PartitionColumnNotFound, columnName, cond.toString());
                }
                frag.getCondition().add(parse(cond, col));
            }
            // TODO: 此处应检查condition中左值和右值的类型
            table.getFragment().put(partitionName, frag);
        }
        return ErrorMessage.getOKMessage();
    }

    private Condition parse(RangePartitionElementCondition cond, Column col)
    {
        Condition condition = new Condition();
        condition.setColumnName(col.getColumnName());
        switch(cond.getPartitionPredicate()) {
            case EQUAL:
                condition.setCompareType(GddUtil.compareEQUAL);
                break;
            case GREATER:
                condition.setCompareType(GddUtil.compareGREAT);
                break;
            case GREATEREQ:
                condition.setCompareType(GddUtil.compareGREATEQUAL);
                break;
            case LESS:
                condition.setCompareType(GddUtil.compareLESS);
                break;
            case LESSEQ:
                condition.setCompareType(GddUtil.compareLESSEQUAL);
                break;
            case NULL:
                condition.setCompareType(GddUtil.compareNOTEQUAL);
                break;
        }
        condition.setValue(cond.getPartitionExpr().toString());
        condition.setDataType(col.getDataType());
        return condition;
    }

    @Override
    public boolean isAlreadyDone()
    {
        return alreadyDone;
    }
}
