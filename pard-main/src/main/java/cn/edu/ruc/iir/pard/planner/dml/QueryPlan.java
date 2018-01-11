package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.executor.connector.node.DistinctNode;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.JoinNode;
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
import cn.edu.ruc.iir.pard.sql.expr.ColumnItem;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.FalseExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
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
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Query Plan
 * A query statement can be translated to a query plan.
 * @author hagen
 */
public class QueryPlan
        extends Plan implements EarlyStopPlan
{
    private Logger logger = Logger.getLogger(QueryPlan.class.getName());
    private PlanNode node;
    private boolean alreadyDone;
    private Optional<LimitNode> limit;
    private Optional<SortNode> sort;
    private Optional<DistinctNode> distinct;
    private ProjectNode project;
    private Optional<FilterNode> filter;
    private UnionNode union;
    public QueryPlan(Statement stmt)
    {
        super(stmt);
    }
    public PlanNode getPlan()
    {
        return node;
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        logger = Logger.getLogger(QueryPlan.class.getName());
        node = new OutputNode();
        ColumnItem.clearCol2TblMap();
        Map<String, String> col2tbl = ColumnItem.getCol2TblMap();
        this.limit = Optional.ofNullable(null);
        this.sort = Optional.ofNullable(null);
        this.distinct = Optional.ofNullable(null);
        this.project = null;
        this.filter = Optional.ofNullable(null);
        this.union = null;
        PlanNode currentNode = this.node;
        System.out.println("currentNode" + currentNode);
        // get real objects
        Query query = (Query) this.getStatment();
        QueryBody queryBody = query.getQueryBody();
        if (!(queryBody instanceof QuerySpecification)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, "Query Body is not a query specification!");
        }
        QuerySpecification querySpecification = (QuerySpecification) queryBody;
        Select select = querySpecification.getSelect();
        if (!querySpecification.getFrom().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, " FROM is missing!");
        }
        Relation from = querySpecification.getFrom().get();
        if (!(from instanceof Table)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, " FROM ITEM is not a table!");
        }
        if (querySpecification.getGroupBy().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, " Group by not supported at present!");
        }
        if (querySpecification.getHaving().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, " Having is not supported!");
        }

        // check schema and table
        Table fromTable = (Table) from;
        cn.edu.ruc.iir.pard.catalog.Table catalogTable;
        String schemaName = null;
        boolean checkSchema = false;
        Schema schema = null;
        if (fromTable.getName().getPrefix().isPresent()) {
            schemaName = fromTable.getName().getPrefix().get().toString();
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
        List<String> siteList = new ArrayList<String>();

        TableDao tableDao = new TableDao(schema);
        catalogTable = tableDao.loadByName(fromTableName);
        if (catalogTable == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.TableNotExists, schemaName + "." + fromTableName);
        }
        SiteDao sdao = new SiteDao();
        int pos = fromTableName.indexOf("@");
        if (pos > 0) {
            String site = fromTableName.substring(pos + 1);
            if (sdao.loadByName(site) == null) {
                return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SiteNotExist, site);
            }
            siteList.add(site);
            fromTableName = fromTableName.substring(0, pos);
        }
        else {
            //siteList.addAll(sdao.listNodes().keySet());
            for (Fragment frag : catalogTable.getFragment().values()) {
                siteList.add(frag.getSiteName());
            }
        }
        for (Column col : catalogTable.getColumns().values()) {
            col2tbl.put(col.getColumnName(), fromTableName);
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
            limit = Optional.of(limitNode);
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
            sort = Optional.of(sortNode);
            currentNode = sortNode;
        }
        // distinct and project
        boolean hasAllColumn = false;
        List<SelectItem> selectItems = select.getSelectItems();
        List<Column> columns = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof AllColumns) {
                columns.clear();
                hasAllColumn = true;
                columns.addAll(catalogTable.getColumns().values());
                break;
            }
            else {
                Expression expression = ((SingleColumn) selectItem).getExpression();
                if (!(expression instanceof Identifier)) {
                    return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, "expession " + expression.toString() + " is not supported!");
                }
                Identifier identifier = (Identifier) expression;
                if (catalogTable.getColumns().keySet().contains(identifier.getValue())) {
                    columns.add(catalogTable.getColumns().get(identifier.getValue()));
                }
            }
        }
        if (select.isDistinct()) {
            DistinctNode distinctNode = new DistinctNode(columns);
            distinct = Optional.of(distinctNode);
            currentNode.setChildren(distinctNode, true, true);
            currentNode = distinctNode;
        }
        ProjectNode projectNode = new ProjectNode(columns);
        currentNode.setChildren(projectNode, true, true);
        project = projectNode;
        currentNode = projectNode;

        // filter
        if (querySpecification.getWhere().isPresent()) {
            Expression filterExpr = querySpecification.getWhere().get();
            FilterNode filterNode = new FilterNode(filterExpr);
            filter = Optional.of(filterNode);
            //currentNode.setChildren(filterNode, true, true);
            //currentNode = filterNode;
        }
        // scan
        UnionNode node = horizonLocalization(tableDao, siteList, fromTableName, hasAllColumn);
        currentNode.setChildren(node, true, true);
        currentNode = node;
        logger.info("Parsed query plan: " + node.toString());
        //col2tblMap.remove();
        return ErrorMessage.throwMessage(ErrorMessage.ErrCode.OK);
    }
    public JoinNode verticalLocalization(TableDao tdao, List<String> siteList, String fromTableName, boolean projectColumn)
    {
        //TODO: join localization
        return null;
    }
    public UnionNode horizonLocalization(TableDao tdao, List<String> siteList, String fromTableName, boolean projectColumn)
    {
        UnionNode unionNode = new UnionNode();
        cn.edu.ruc.iir.pard.catalog.Table catalogTable = tdao.loadByName(fromTableName);
        //union = unionNode;
        for (Fragment frag : catalogTable.getFragment().values()) {
            if (!siteList.contains(frag.getSiteName())) {
                continue;
            }
            Expr expr = Expr.parse(frag.getCondition(), fromTableName);
            PlanNode childrenNode = new TableScanNode(tdao.getSchemaName(), fromTableName, frag.getSiteName());
            if (filter.isPresent()) {
                //TODO: 从expr2中选择自己的
                Expr expr2 = Expr.parse(filter.get().getExpression());
                Expr merge = Expr.and(expr, expr2, LogicOperator.AND);
                if (merge instanceof TrueExpr) {
                    // do nothing.
                }
                else if (merge instanceof FalseExpr) {
                    continue;
                }
                else {
                    //merge = Expr.and(expr, expr2, LogicOperator.OR);
                    FilterNode childrenFilter = new FilterNode(merge.toExpression());
                    childrenFilter.setChildren(childrenNode, true, true);
                    childrenNode = childrenFilter;
                }
            }
            if (project != null && !projectColumn) {
                //TODO: 选择自己表里元素下推
                ProjectNode pnode = new ProjectNode(project.getColumns());
                pnode.setChildren(childrenNode, true, true);
                childrenNode = pnode;
            }
            if (distinct.isPresent()) {
              //TODO: 选择自己表里元素distinct
                DistinctNode dnode = new DistinctNode(distinct.get().getColumns());
                dnode.setChildren(childrenNode, true, true);
                childrenNode = dnode;
            }
            unionNode.addUnionChild(childrenNode);
        }
        if (unionNode.getUnionChildren().isEmpty()) {
            //this.alreadyDone = true;
        }
        return unionNode;
    }
    public PlanNode optimize()
    {
        return node;
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
