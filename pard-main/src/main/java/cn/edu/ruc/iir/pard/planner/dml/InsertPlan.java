package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertPlan
        extends Plan
{
    private String schemaName = null;
    private String tableName = null;
    private Table table = null;
    private List<Column> colList = null;
    private Map<String, Object> distributionHints = null;
    public InsertPlan(Statement stmt)
    {
        super(stmt);
        distributionHints = new HashMap<String, Object>();
    }
    @Override
    public ErrorMessage semanticAnalysis()
    {
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
        // prepared for col.
        colList = new ArrayList<Column>();
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
                    colList.add(col);
                }
            }
        }
        else {
            for (String key : table.getColumns().keySet()) {
                colList.add(table.getColumns().get(key));
            }
            colList.sort((x, y) -> x.getId() - y.getId());
        }
        Query q = insert.getQuery();
        QueryBody qb = q.getQueryBody();
        if (!(qb instanceof Values)) {
            return ErrorMessage.throwMessage(ErrCode.InsertFromSelectNotImplemented);
        }
        Values values = (Values) qb;
        for (Expression expr : values.getRows()) {
            if (!(expr instanceof Row)) {
                return ErrorMessage.throwMessage(ErrCode.InsertExpectedRow);
            }
            Row row = (Row) expr;
            if (row.getItems().size() != colList.size()) {
                return ErrorMessage.throwMessage(ErrCode.InsertRowValuesNotMatchColumns, row.getItems().size(), colList.size());
            }
            for (int i = 0; i < row.getItems().size(); i++) {
                Expression item = row.getItems().get(i);
                System.out.println(item.getClass().getName());
                Literal literal = (Literal) item;
                if (!typeMatch(colList.get(i), literal)) {
                    return ErrorMessage.throwMessage(ErrCode.ValuesTypeNotMatch, literal.toString());
                }
            }
        }
        return null;
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
    public List<Column> getColList()
    {
        return colList;
    }
    @Override
    public Map<String, Object> getDistributionHints()
    {
        return this.distributionHints;
    }
}
