package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.executor.connector.node.DistinctNode;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.LimitNode;
import cn.edu.ruc.iir.pard.executor.connector.node.OutputNode;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.ProjectNode;
import cn.edu.ruc.iir.pard.executor.connector.node.SortNode;
import cn.edu.ruc.iir.pard.executor.connector.node.TableScanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.UnionNode;
import cn.edu.ruc.iir.pard.planner.EarlyStopPlan;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.sql.tree.AllColumns;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.QueryBody;
import cn.edu.ruc.iir.pard.sql.tree.QuerySpecification;
import cn.edu.ruc.iir.pard.sql.tree.Relation;
import cn.edu.ruc.iir.pard.sql.tree.Select;
import cn.edu.ruc.iir.pard.sql.tree.SelectItem;
import cn.edu.ruc.iir.pard.sql.tree.SingleColumn;
import cn.edu.ruc.iir.pard.sql.tree.SortItem;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.sql.tree.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Query Plan
 * A query statement can be translated to a query plan.
 * @author hagen
 */
public class QueryPlan
        extends Plan implements EarlyStopPlan
{
    private final PlanNode node;
    private boolean alreadyDone = false;

    public QueryPlan(Statement stmt)
    {
        super(stmt);
        this.node = new OutputNode();
    }

    public PlanNode getPlan()
    {
        return node;
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        PlanNode currentNode = node;

        // get real objects
        Query query = (Query) this.getStatment();
        QueryBody queryBody = query.getQueryBody();
        if (!(queryBody instanceof QuerySpecification)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery);
        }
        QuerySpecification querySpecification = (QuerySpecification) queryBody;
        Select select = querySpecification.getSelect();
        if (!querySpecification.getFrom().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery);
        }
        Relation from = querySpecification.getFrom().get();
        if (!(from instanceof Table)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery);
        }
        if (querySpecification.getGroupBy().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery);
        }
        if (querySpecification.getHaving().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery);
        }

        // check schema and table
        Table fromTable = (Table) from;
        cn.edu.ruc.iir.pard.catalog.Table catalogTable;
        String schemaName = null;
        boolean checkSchema = false;
        Schema schema = null;
        if (fromTable.getName().getPrefix().isPresent()) {
            schemaName = fromTable.getName().getPrefix().toString();
        }
        String fromTableName = fromTable.getName().getSuffix();
        if (schemaName == null) {
            schema = UsePlan.getCurrentSchema();
            if (schema != null) {
                schemaName = schema.getName();
                checkSchema = true;
            }
        }
        if (schemaName == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SchemaNotSpecified);
        }
        if (!checkSchema) {
            SchemaDao schemaDao = new SchemaDao();
            schema = schemaDao.loadByName(schemaName);
            if (schema == null) {
                return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SchemaNotExsits, schemaName);
            }
        }
        TableDao tableDao = new TableDao(schema);
        catalogTable = tableDao.loadByName(fromTableName);
        if (catalogTable == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.TableNotExists, schemaName + "." + fromTableName);
        }

        // construct operator tree
        // limit
        if (query.getLimit().isPresent()) {
            int limitVal;
            try {
                limitVal = Integer.parseInt(query.getLimit().get());
            }
            catch (Exception e) {
                return ErrorMessage.throwMessage(ErrorMessage.ErrCode.LimitIsNotANumber);
            }
            LimitNode limitNode = new LimitNode(limitVal);
            currentNode.setChildren(limitNode, true, true);
            currentNode = limitNode;
        }
        // order by
        if (query.getOrderBy().isPresent()) {
            SortNode sortNode = new SortNode();
            for (SortItem sortItem : query.getOrderBy().get().getSortItems()) {
                Identifier sortKey = (Identifier) sortItem.getSortKey();
                if (catalogTable.getColumns().containsKey(sortKey.getValue())) {
                    Column sortCol = catalogTable.getColumns().get(sortKey.getValue());
                    sortNode.addSort(sortCol, sortItem.getOrdering() == SortItem.Ordering.ASCENDING);
                }
                else {
                    return ErrorMessage.throwMessage(ErrorMessage.ErrCode.ColumnInTableNotFound);
                }
            }
            currentNode.setChildren(sortNode, true, true);
            currentNode = sortNode;
        }
        // distinct and project
        List<SelectItem> selectItems = select.getSelectItems();
        List<Column> columns = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof AllColumns) {
                columns.clear();
                columns.addAll(catalogTable.getColumns().values());
                break;
            }
            else {
                Expression expression = ((SingleColumn) selectItem).getExpression();
                if (!(expression instanceof Identifier)) {
                    return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery);
                }
                Identifier identifier = (Identifier) expression;
                if (catalogTable.getColumns().keySet().contains(identifier.getValue())) {
                    columns.add(catalogTable.getColumns().get(identifier.getValue()));
                }
            }
        }
        if (select.isDistinct()) {
            DistinctNode distinctNode = new DistinctNode(columns);
            currentNode.setChildren(distinctNode, true, true);
            currentNode = distinctNode;
        }
        ProjectNode projectNode = new ProjectNode(columns);
        currentNode.setChildren(projectNode, true, true);
        currentNode = projectNode;

        // filter
        if (querySpecification.getWhere().isPresent()) {
            Expression filterExpr = querySpecification.getWhere().get();
            FilterNode filterNode = new FilterNode(filterExpr);
            currentNode.setChildren(filterNode, true, true);
            currentNode = filterNode;
        }

        // scan
        UnionNode unionNode = new UnionNode();
        currentNode.setChildren(unionNode, true, true);
        // todo check for sites that really need query execution.
        SiteDao siteDao = new SiteDao();
        for (String site : siteDao.listNodes().keySet()) {
            TableScanNode scanNode = new TableScanNode(schemaName, fromTableName, site);
            unionNode.addUnionChild(scanNode);
        }

        return ErrorMessage.throwMessage(ErrorMessage.ErrCode.OK);
    }

    public void optimize()
    {
        // todo add optimization rules
    }

    @Override
    public boolean isAlreadyDone()
    {
        return alreadyDone;
    }

    public HashMap<String, PlanNode> getDistributionHints()
    {
        return new HashMap<>();
    }
}
