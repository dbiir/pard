package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.commons.exception.ErrorMessage;
import cn.edu.ruc.iir.pard.commons.exception.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ConditionComparator;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.CharLiteral;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Insert;
import cn.edu.ruc.iir.pard.sql.tree.Literal;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.NullLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.QueryBody;
import cn.edu.ruc.iir.pard.sql.tree.Row;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Values;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertPlan
        extends Plan
{
    private String schemaName;
    private String tableName;
    private Table table = null;
    private Map<String, List<Column>> colListMap;
    private Map<String, List<Row>> distributionHints;

    public InsertPlan(Statement stmt)
    {
        super(stmt);
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        List<Column> colLists = new ArrayList<>();
        distributionHints = new HashMap<>();
        colListMap = new HashMap<>();
        Statement statement = this.getStatment();
        Schema schema = null;
        if (!(statement instanceof Insert)) {
            return ErrorMessage.throwMessage(ErrCode.ParseError, "insert statement.");
        }
        Insert insert = (Insert) statement;
        tableName = insert.getTarget().getSuffix();
        if (insert.getTarget().getPrefix().isPresent()) {
            schemaName = insert.getTarget().getPrefix().get().toString();
        }
        boolean checkSchema = false;
        if (schemaName == null) {
            schema = UsePlan.getCurrentSchema();
            if (schema != null) {
                schemaName = schema.getName();
                checkSchema = true;
            }
        }
        if (schemaName == null) {
            return ErrorMessage.throwMessage(ErrCode.SchemaNotSpecified);
        }
        if (!checkSchema) {
            SchemaDao schemaDao = new SchemaDao();
            schema = schemaDao.loadByName(schemaName);
            if (schema == null) {
                return ErrorMessage.throwMessage(ErrCode.SchemaNotExsits, schemaName);
            }
        }
        TableDao tableDao = new TableDao(schema);
        table = tableDao.loadByName(tableName);
        if (table == null) {
            return ErrorMessage.throwMessage(ErrCode.TableNotExists, schemaName + "." + tableName);
        }
        Map<String, List<String>> col2site = new HashMap<String, List<String>>();
        List<String> siteList = new ArrayList<String>();
        table.getFragment().values().forEach(x->siteList.add(x.getSiteName()));
        //TODO: 混合分区需要修改
        boolean isHorizontal = true;
        if (table.getFragment().values().iterator().next().getFragmentType() == GddUtil.fragementHORIZONTIAL) {
            isHorizontal = true;
            for (String site : siteList) {
                colListMap.put(site, new ArrayList<>());
            }
            for (Column col : table.getColumns().values()) {
                col2site.put(col.getColumnName(), siteList);
            }
        }
        else {
            isHorizontal = false;
            for (Column col : table.getColumns().values()) {
                col2site.put(col.getColumnName(), new ArrayList<>());
            }
            for (Fragment frag : table.getFragment().values()) {
                colListMap.put(frag.getSiteName(), new ArrayList<>());
                String siteName = frag.getSiteName();
                for (Condition cond : frag.getCondition()) {
                    col2site.get(cond.getColumnName()).add(siteName);
                }
            }
        }
        // prepared for col.
        if (insert.getColumns().isPresent()) {
            List<String> colStrList = insert.getColumns().get();
            int i = 0;
            for (; i < colStrList.size(); i++) {
                String colStr = colStrList.get(i);
                Column col = table.getColumns().get(colStr);
                if (col == null) {
                    return ErrorMessage.throwMessage(ErrCode.ColumnInTableNotFound, colStr, tableName);
                }
                else {
                    colLists.add(col);
                    List<String> sites = col2site.get(col.getColumnName());
                    for (String site : sites) {
                        colListMap.get(site).add(col);
                    }
                }
            }
        }
        else {
            for (String key : table.getColumns().keySet()) {
                colLists.add(table.getColumns().get(key));
                Column col = table.getColumns().get(key);
                List<String> sites = col2site.get(col.getColumnName());
                for (String site : sites) {
                    colListMap.get(site).add(col);
                }
            }
            for (String site : siteList) {
                colListMap.get(site).sort(Comparator.comparingInt(Column::getId));
            }
            colLists.sort(Comparator.comparingInt(Column::getId));
        }
        Query q = insert.getQuery();
        QueryBody qb = q.getQueryBody();
        if (!(qb instanceof Values)) {
            return ErrorMessage.throwMessage(ErrCode.InsertFromSelectNotImplemented);
        }
        Values values = (Values) qb;
        for (String key : table.getFragment().keySet()) {
            distributionHints.put(table.getFragment().get(key).getSiteName(), new ArrayList<>());
        }
        for (Expression expr : values.getRows()) {
            if (!(expr instanceof Row)) {
                return ErrorMessage.throwMessage(ErrCode.InsertExpectedRow);
            }
            Row row = (Row) expr;
            if (row.getItems().size() != colLists.size()) {
                return ErrorMessage.throwMessage(ErrCode.InsertRowValuesNotMatchColumns, row.getItems().size(), colLists.size());
            }
            Map<String, Literal> literalMap = new HashMap<String, Literal>();
            Map<String, Row> distRow = new HashMap<>();
            for (String key : distributionHints.keySet()) {
                distRow.put(key, new Row(new ArrayList<>()));
            }
            for (int i = 0; i < row.getItems().size(); i++) {
                Expression item = row.getItems().get(i);
                //System.out.println(item.getClass().getName());
                Literal literal = (Literal) item;
                if (!typeMatch(colLists.get(i), literal)) {
                    return ErrorMessage.throwMessage(ErrCode.ValuesTypeNotMatch, literal.toString());
                }
                Column col = colLists.get(i);
                literalMap.put(col.getColumnName(), literal);
                if (!isHorizontal) {
                    for (String site : col2site.get(col.getColumnName())) {
                        distRow.get(site).getItems().add(item);
                    }
                }
            }
            for (String key : distRow.keySet()) {
                Row rowTmp = distRow.get(key);
                if (rowTmp != null && !rowTmp.getItems().isEmpty()) {
                    distributionHints.get(key).add(rowTmp);
                }
            }
            if (isHorizontal) {
                for (String key : table.getFragment().keySet()) {
                    Fragment f = table.getFragment().get(key);
                    boolean belongTo = ConditionComparator.matchLiteral(f.getCondition(), literalMap);
                    //System.out.println(belongTo + " " + key);
                    if (belongTo) {
                        List<Row> o = distributionHints.get(f.getSiteName());
                        List<Row> rlist = null;
                        if (o == null) {
                            rlist = new ArrayList<Row>();
                            distributionHints.put(f.getSiteName(), rlist);
                        }
                        else {
                            rlist = o;
                        }
                        rlist.add(row);
                    }
                }
            }
        }
        return ErrorMessage.getOKMessage();
    }

    //TODO: type check.
    public boolean typeMatch(Column col, Literal literal)
    {
        if (literal instanceof LongLiteral) {
            // check literal type
        }
        else
            if (literal instanceof DoubleLiteral) {
             // check literal type
            }
            else
                if (literal instanceof BooleanLiteral) {
                 // check literal type
                }
                else
                    if (literal instanceof CharLiteral) {
                     // check literal type
                    }
                    else
                        if (literal instanceof NullLiteral) {
                         // check literal type
                        }
                        else
                            if (literal instanceof StringLiteral) {
                             // check literal type
                            }
        return true;
    }

    public Map<String, List<Column>> getColListMap()
    {
        return colListMap;
    }

    public Map<String, List<Row>> getDistributionHints()
    {
        return this.distributionHints;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }
}
