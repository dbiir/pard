package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// todo remove unnecessary table creations in distributionHints
// todo add support for vertical partitions
public class TableCreationPlan
        extends TablePlan
{
    private Map<String, List<Column>> distributionHints;
    private CreateTable stmt;
    private String tableName;
    private String schemaName;
    private Table table;
    private boolean isNotExists;
    private TableDao tableDao;
    private Map<String, Column> vpmap;
    public TableCreationPlan(Statement stmt)
    {
        super(stmt);
    }

    private List<Column> colList;
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
        vpmap = new HashMap<String, Column>();
        //colListMap = new HashMap<String, List<Column>>();
        colList = new ArrayList<Column>();
        distributionHints = new HashMap<>();
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
                    ColumnDefinition cd = (ColumnDefinition) e;
                    if (!list.contains(cd)) {
                        list.add((ColumnDefinition) e);
                    }
                }
                else {
                    return ErrorMessage.throwMessage(ErrCode.NotaColumnDefinition, e.toString());
                }
            }
        }
        table = new Table();
        table.setTablename(tableName);
        for (ColumnDefinition cd : list) {
            //List<Column> cols = vpmap.get(cd);
            String type = cd.getType();
            String colName = cd.getName().toString();

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
            //System.out.println("type:" + type + " colName: " + colName + " col id " + column.getId());
            if (table.getColumns().get(colName) == null) {
                vpmap.put(cd.getName().toString(), column);
                //System.out.println("put " + cd.getName().toString() + " to vp");
                colList.add(column);
                table.getColumns().put(column.getColumnName(), column);
            }
        }

        // check partition
        if (hp == null && !vp.isEmpty()) {
            return parseVPartition(vp);
        }
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
    public ErrorMessage parseVPartition(List<TableVPartitioner> vp)
    {
        SiteDao siteDao = new SiteDao();
        for (TableVPartitioner v : vp) {
            String siteName = v.getNodeId();
            Site site = siteDao.loadByName(siteName);
            if (site == null) {
                return ErrorMessage.throwMessage(ErrCode.SiteNotExist, siteName);
            }
            Fragment frag = new Fragment();
            frag.setFragmentName(v.getNodeId());
            frag.setFragmentType(GddUtil.fragmentVERTICAL);
            frag.setSiteName(site.getName());
            frag.setSubTable(null);
            List<Column> vpColList = new ArrayList<>();
            for (TableElement e : v.getElements()) {
                if (e instanceof ColumnDefinition) {
                    ColumnDefinition cd = (ColumnDefinition) e;
                    vpColList.add(vpmap.get(cd.getName().toString()));
                    if (vpmap.get(cd.getName().toString()) == null) {
                        System.out.println(cd.getName().toString() + " not found.");
                    }
                    frag.getCondition().add(new Condition(cd.getName().getValue(), 0, "0", cd.getType().hashCode()));
                }
            }
            vpColList.sort(Comparator.comparingInt(Column::getId));
            table.getFragment().put(frag.getFragmentName(), frag);
            distributionHints.put(siteName, vpColList);
        }
        return ErrorMessage.getOKMessage();
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
        //Map<String, Site> siteMap = (new GDDDao()).load().getSiteMap();
/*
        for (String key : siteMap.keySet()) {
            Site site = siteMap.get(key);
            distributionHints.put(site.getName(), colList);
        }
        */
        for (Fragment frag : table.getFragment().values()) {
            distributionHints.put(frag.getSiteName(), colList);
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

    public String getTableName()
    {
        return tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public boolean isNotExists()
    {
        return isNotExists;
    }

    public Table getTable()
    {
        return table;
    }
    public Map<String, List<Column>> getDistributionHints()
    {
        return distributionHints;
    }
}
