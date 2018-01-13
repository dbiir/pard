package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.GddUtil;
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
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.semantic.SemanticException;
import cn.edu.ruc.iir.pard.sql.expr.ColumnItem;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.FalseExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
import cn.edu.ruc.iir.pard.sql.tree.AllColumns;
import cn.edu.ruc.iir.pard.sql.tree.DereferenceExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.Join;
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
import cn.edu.ruc.iir.pard.web.PardServlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Query Plan
 * A query statement can be translated to a query plan.
 * @author hagen
 */
public class QueryPlan2
        extends QueryPlan implements EarlyStopPlan
{
    private Logger logger = Logger.getLogger(QueryPlan2.class.getName());
    private PlanNode node;
    private boolean alreadyDone;
    private Optional<LimitNode> limit;
    private Optional<SortNode> sort;
    private Optional<DistinctNode> distinct;
    private ProjectNode project;
    private Optional<JoinNode> join;
    private Optional<FilterNode> filter;

    // for projection
    private Map<String, Column> alias2col; // 列名别名与列的对应
    private Map<String, cn.edu.ruc.iir.pard.catalog.Table> colAlias2tbl; // 列名的别名与表的对应关系
    private Map<String, String> alias2tbl;
    private List<Column> colList; // projection中列的顺序

    // for from
    private Map<String, cn.edu.ruc.iir.pard.catalog.Column> fullColAlias2col; // 使用到的所有的表的列 key: tbl.col[string] value: col[Column]
    private Map<String, String> col2tbl; //使用到的所有的表的列 key: tbl.col[string] value: tbl[string]
    private Map<String, List<String>> col2tblList;
    private Map<String, String> tbl2schema; //使用到的表与schema的对应关系
    private Map<String, cn.edu.ruc.iir.pard.catalog.Table> catalog; // 使用到的所有表
    private Map<String, TableDao> tableDaoMap;
    //for site
    private Map<String, List<String>> tbl2site;

 //   private UnionNode union;
    private static SiteDao siteDao;
    private boolean siteMayMissing;
    private List<String> aliveSite;

    private List<String> tableList; // 使用的表的顺序

    private boolean selectPushDown;
    public QueryPlan2(Statement stmt)
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
        siteDao = new SiteDao();
        aliveSite = new ArrayList<String>();
        aliveSite.addAll(siteDao.listNodes().keySet());
        siteMayMissing = false;
        catalog = new HashMap<>();
        alias2col = new HashMap<String, Column>();
        colAlias2tbl = new HashMap<String, cn.edu.ruc.iir.pard.catalog.Table>();
        tbl2schema = new HashMap<String, String>();
        col2tbl = new HashMap<String, String>();
        tableList = new ArrayList<>();
        colList = new ArrayList<>();
        logger = Logger.getLogger(QueryPlan2.class.getName());
        node = new OutputNode();
        selectPushDown = false;
        tbl2site = new HashMap<>();
        fullColAlias2col = new HashMap<>();
        col2tblList = new HashMap<>();
        tableDaoMap = new HashMap<>();

        ColumnItem.clearCol2TblMap();
        alias2tbl = ColumnItem.getCol2TblMap();
        this.limit = Optional.ofNullable(null);
        this.sort = Optional.ofNullable(null);
        this.distinct = Optional.ofNullable(null);
        this.project = null;
        this.filter = Optional.ofNullable(null);
        this.join = Optional.ofNullable(null);
   //     this.union = null;
        PlanNode currentNode = this.node;
        //System.out.println("currentNode" + currentNode);
        // get real objects
        Query query = (Query) this.getStatment();
        QueryBody queryBody = query.getQueryBody();
        if (!(queryBody instanceof QuerySpecification)) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, "Query Body is not a query specification!");
        }
        QuerySpecification querySpecification = (QuerySpecification) queryBody;
        Select select = querySpecification.getSelect();
        if (querySpecification.getGroupBy().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, " Group by not supported at present!");
        }
        if (querySpecification.getHaving().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, " Having is not supported!");
        }
        if (!querySpecification.getFrom().isPresent()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery, " FROM is missing!");
        }
        //check for schema
        String schemaName = null;
        //boolean checkSchema = false;
        Schema schema = null;
        schema = UsePlan.getCurrentSchema();
        if (schema != null) {
            schemaName = schema.getName();
            //checkSchema = true;
        }
        // check for projection
        //check for table
        Relation from = querySpecification.getFrom().get();
        checkRelation(from, schemaName, schema);
        fillAlias2tbl();
        currentNode = checkLimit(query, currentNode);
        currentNode = checkOrderBy(query, currentNode);
        currentNode = checkProjectionAndDistinct(select, currentNode);
        currentNode = checkFilter(querySpecification, currentNode);
        if (tableList.size() == 1) {
            PlanNode unionNode = localization(tableList.get(0));
            currentNode.setChildren(unionNode, true, true);
            currentNode = unionNode;
        }
        else {
            PlanNode joinNode = join();
            currentNode.setChildren(joinNode, true, true);
            currentNode = joinNode;
        }
        logger.info("Parsed query plan: " + this.node.toString());
        if (!siteMayMissing) {
            return ErrorMessage.getOKMessage();
        }
        else {
            return ErrorMessage.throwMessage(ErrCode.SomeSiteDown);
        }
    }
    public void fillAlias2tbl()
    {
        for (String key : col2tblList.keySet()) {
            List<String> tb = col2tblList.get(key);
            if (tb.size() == 1) {
                alias2tbl.put(key, tb.get(0));
            }
        }
    }
    public PlanNode join()
    {
        JoinNode join = new JoinNode();
        for (String tableName : tableList) {
            PlanNode node = localization(tableName);
            if (node != null) {
                join.addJoinChild(node);
            }
            else {
                return null;
            }
        }
        return join;
    }
    public PlanNode localization(String fromTableName)
    {
        cn.edu.ruc.iir.pard.catalog.Table catalogTable = catalog.get(fromTableName);
        if (catalogTable.getFragment().values().iterator().next().getFragmentType() == GddUtil.fragementHORIZONTIAL) {
            UnionNode union = horizonLocalization(fromTableName);
            if (union.getUnionChildren().isEmpty()) {
                return null;
            }
            return union;
        }
        else {
            JoinNode join = verticalLocalization(fromTableName);
            if (join.getJoinChildren().isEmpty()) {
                return null;
            }
            return join;
        }
    }

    public PlanNode checkFilter(QuerySpecification querySpecification, PlanNode currentNode)
    {
        if (querySpecification.getWhere().isPresent()) {
            Expression filterExpr = querySpecification.getWhere().get();
            FilterNode filterNode = new FilterNode(filterExpr);
            filter = Optional.of(filterNode);
            currentNode.setChildren(filterNode, true, true);
            currentNode = filterNode;
        }
        return currentNode;
    }
    public PlanNode checkProjectionAndDistinct(Select select, PlanNode currentNode)
    {
        List<SelectItem> selectItems = select.getSelectItems();
        //List<Column> columns = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof AllColumns) {
                colList.clear();
                selectPushDown = true;
                colList.addAll(fullColAlias2col.values());
                colAlias2tbl.clear();
                alias2col.clear();
                alias2tbl.clear();
                for (String table : tableList) {
                    cn.edu.ruc.iir.pard.catalog.Table catalogTable = catalog.get(table);
                    for (Column c : catalogTable.getColumns().values()) {
                        colAlias2tbl.put(c.getColumnName(), catalogTable);
                        alias2col.put(c.getColumnName(), c);
                        alias2tbl.put(c.getColumnName(), catalogTable.getTablename());
                    }
                }

                break;
            }
            else {
                Expression expression = ((SingleColumn) selectItem).getExpression();
                if (expression instanceof Identifier) {
                    //throw new SemanticException(ErrorMessage.ErrCode.UnSupportedQuery, "expession " + expression.toString() + " is not supported! class " + expression.getClass().getName());
                    //TODO： 处理
                    Identifier identifier = (Identifier) expression;
                    String alias = identifier.getValue();
                    List<String> tblColLists1 = col2tblList.get(alias);
                    if (tblColLists1 == null) {
                        throw new SemanticException(ErrCode.ColumnInTableNotFound, identifier.getValue(), " multi-table");
                    }
                    else if (tblColLists1.size() >= 2) {
                        throw new SemanticException(ErrCode.ColumnNameIsAmbiguous, identifier.getValue(), tblColLists1.get(0) + " and " + tblColLists1.get(1));
                    }
                    cn.edu.ruc.iir.pard.catalog.Table catalogTable = catalog.get(tblColLists1.get(0));
                    if (catalogTable.getColumns().keySet().contains(identifier.getValue())) {
                        Column col = catalogTable.getColumns().get(identifier.getValue());
                        colList.add(col);
                        if (colAlias2tbl.get(alias) != null) {
                            throw new SemanticException(ErrCode.ColumnNameIsAmbiguous, identifier.getValue(), colAlias2tbl.get(alias).getTablename() + " and  " + catalogTable.getTablename());
                        }
                        colAlias2tbl.put(alias, catalogTable);
                        alias2col.put(alias, col);
                        alias2tbl.put(alias, catalogTable.getTablename());
                    }
                    else {
                        throw new SemanticException(ErrCode.ColumnInTableNotFound, identifier.getValue(), catalogTable);
                    }
                }
                else if (expression instanceof DereferenceExpression) {
                    DereferenceExpression exp = (DereferenceExpression) expression;
                    Identifier identifier = exp.getField();
                    String tblName = exp.getBase().toString();
                    String alias = exp.toString();
                    cn.edu.ruc.iir.pard.catalog.Table catalogTable = catalog.get(tblName.toLowerCase());
                    if (catalogTable.getColumns().keySet().contains(identifier.getValue())) {
                        Column col = catalogTable.getColumns().get(identifier.getValue());
                        colList.add(col);
                        if (colAlias2tbl.get(alias) != null) {
                            throw new SemanticException(ErrCode.ColumnNameIsAmbiguous, identifier.getValue(), colAlias2tbl.get(alias).getTablename() + " and  " + catalogTable.getTablename());
                        }
                        colAlias2tbl.put(alias, catalogTable);
                        alias2col.put(alias, col);
                        alias2tbl.put(alias, catalogTable.getTablename());
                    }
                }
                else {
                    throw new SemanticException(ErrorMessage.ErrCode.UnSupportedQuery, "expession " + expression.toString() + " is not supported! class " + expression.getClass().getName());
                }
            }
        }
        if (select.isDistinct()) {
            DistinctNode distinctNode = new DistinctNode(colList);
            distinct = Optional.of(distinctNode);
            currentNode.setChildren(distinctNode, true, true);
            currentNode = distinctNode;
        }
        ProjectNode projectNode = new ProjectNode(colList);
        currentNode.setChildren(projectNode, true, true);
        project = projectNode;
        currentNode = projectNode;
        return currentNode;
    }
    public PlanNode checkOrderBy(Query query, PlanNode currentNode)
    {
        if (query.getOrderBy().isPresent()) {
            SortNode sortNode = new SortNode();
            for (SortItem sortItem : query.getOrderBy().get().getSortItems()) {
                Identifier sortKey = (Identifier) sortItem.getSortKey();
                if (col2tbl.get(sortKey.getValue()) != null) {
                    Column sortCol = catalog.get(col2tbl.get(sortKey.getValue())).getColumns().get(sortKey.getValue());
                    sortNode.addSort(sortCol, sortItem.getOrdering() == SortItem.Ordering.ASCENDING);
                }
                else {
                    throw new SemanticException(ErrorMessage.ErrCode.ColumnInTableNotFound);
                }
            }
            currentNode.setChildren(sortNode, true, true);
            sort = Optional.of(sortNode);
            currentNode = sortNode;
        }
        return currentNode;
    }
    public PlanNode checkLimit(Query query, PlanNode currentNode)
    {
        if (query.getLimit().isPresent()) {
            int limitVal;
            try {
                limitVal = Integer.parseInt(query.getLimit().get());
            }
            catch (Exception e) {
                throw new SemanticException(ErrorMessage.ErrCode.LimitIsNotANumber);
            }
            LimitNode limitNode = new LimitNode(limitVal);
            limit = Optional.of(limitNode);
            currentNode.setChildren(limitNode, true, true);
            currentNode = limitNode;
        }
        return currentNode;
    }
    public void checkRelation(Relation from, String defaultSchema, Schema schema)
    {
        if (from instanceof Table) {
            Table fromTable = (Table) from;
            checkTable(fromTable, defaultSchema, schema);
        }
        else if (from instanceof Join) {
            Join join = (Join) from;
            checkJoin(join, defaultSchema, schema);
        }
        else {
            throw new SemanticException(ErrorMessage.ErrCode.UnSupportedQuery, " FROM clause " + from.getClass().getName() + " is not supported!!");
        }
    }
    public void checkJoin(Join join, String defaultSchema, Schema schema)
    {
        Relation lr = join.getLeft();
        Relation rr = join.getRight();
        checkRelation(lr, defaultSchema, schema);
        checkRelation(rr, defaultSchema, schema);
    }
    public cn.edu.ruc.iir.pard.catalog.Table checkTable(Table fromTable, String defaultSchema, Schema schema)
    {
        boolean checkSchema = (defaultSchema != null);
        String schemaName = defaultSchema;
        if (fromTable.getName().getPrefix().isPresent()) {
            schemaName = fromTable.getName().getPrefix().get().toString();
            checkSchema = false;
        }
        String fromTableName = fromTable.getName().getSuffix();
        if (schemaName == null) {
            throw new SemanticException(ErrorMessage.ErrCode.SchemaNotSpecified);
        }
        if (!checkSchema) {
            SchemaDao schemaDao = new SchemaDao();
            schema = schemaDao.loadByName(schemaName);
            if (schema == null) {
                throw new SemanticException(ErrorMessage.ErrCode.SchemaNotExsits, schemaName);
            }
        }
        List<String> siteList = new ArrayList<String>();
        TableDao tableDao = tableDaoMap.get(schemaName);
        if (tableDao == null) {
            tableDao = new TableDao(schema);
            tableDaoMap.put(schemaName, tableDao);
        }
        // check table
        int pos = fromTableName.indexOf("@");
        boolean needLoad = false;
        if (pos > 0) {
            String site = fromTableName.substring(pos + 1);
            if (!aliveSite.contains(site)) {
                throw new SemanticException(ErrorMessage.ErrCode.SiteNotExist, site);
            }
            siteList.add(site);
            fromTableName = fromTableName.substring(0, pos);
        }
        else {
            needLoad = true;
        }
        cn.edu.ruc.iir.pard.catalog.Table catalogTable = tableDao.loadByName(fromTableName);
        if (catalogTable == null) {
            throw new SemanticException(ErrorMessage.ErrCode.TableNotExists, schemaName + "." + fromTableName);
        }
        // check site
        tbl2site.put(fromTableName, siteList);
        if (needLoad) {
            for (Fragment frag : catalogTable.getFragment().values()) {
                siteList.add(frag.getSiteName());
                if (!aliveSite.contains(frag.getSiteName())) {
                    siteMayMissing = true;
                }
            }
            if (siteList.isEmpty()) {
                throw new SemanticException(ErrorMessage.ErrCode.AllSiteDown, fromTableName);
            }
        }
        for (Column col : catalogTable.getColumns().values()) {
            col2tbl.put(fromTableName + "." + col.getColumnName(), fromTableName);
            fullColAlias2col.put(fromTableName + "." + col.getColumnName(), col);
            List<String> tblList = col2tblList.get(col.getColumnName());
            if (tblList == null) {
                tblList = new ArrayList<String>();
                col2tblList.put(col.getColumnName(), tblList);
            }
            tblList.add(catalogTable.getTablename());
        }
        //prepared for next step.
        catalog.put(fromTableName, catalogTable);
        tbl2schema.put(fromTableName, schemaName);
        tableList.add(fromTableName);
        return catalogTable;
    }
    public JoinNode verticalLocalization(String fromTableName)
    {
        List<String> siteList = tbl2site.get(fromTableName);
        cn.edu.ruc.iir.pard.catalog.Table catalogTable = catalog.get(fromTableName);
        JoinNode joinNode = new JoinNode();
        Set<String> tblCol = new HashSet<String>();
        tblCol.addAll(extractColumnNameFromProjection(catalogTable));
        tblCol.addAll(extractColumnNameFromFilter(catalogTable));
        Map<String, Integer> cntCol = new HashMap<String, Integer>();
        for (Fragment frag : catalogTable.getFragment().values()) {
            if (!siteList.contains(frag.getSiteName())) {
                continue;
            }
            List<Column> projectColumn = new ArrayList<Column>();
            List<String> strColumn = new ArrayList<String>();
            for (Condition cond : frag.getCondition()) {
                Integer cnt = cntCol.get(cond.getColumnName());
                if (cnt == null) {
                    cnt = 0;
                }
                cnt++;
                cntCol.put(cond.getColumnName(), cnt);
                Column col = catalogTable.getColumns().get(cond.getColumnName());
                if (tblCol.contains(col.getColumnName())) {
                    projectColumn.add(col);
                    strColumn.add(col.getColumnName());
                }
            }
            if (projectColumn.isEmpty()) {
                continue;
            }
            PlanNode childrenNode = null;
            PlanNode root = null;
            ProjectNode proj = new ProjectNode(projectColumn);
            childrenNode = proj;
            root = proj;
            if (filter.isPresent()) {
                // TODO : 垂直投影filter下推
                Expr filterExpr = Expr.parse(filter.get().getExpression());
                filterExpr = Expr.extractTableFilter(filterExpr, fromTableName);
                filterExpr = Expr.extractTableColumnFilter(filterExpr, strColumn);
                if (!(filterExpr instanceof TrueExpr)) {
                    FilterNode subFilter = new FilterNode(filterExpr.toExpression());
                    childrenNode.setChildren(subFilter, true, true);
                    childrenNode = subFilter;
                }
            }
            TableScanNode scan = new TableScanNode(tbl2schema.get(fromTableName), fromTableName, frag.getSiteName());
            childrenNode.setChildren(scan, true, true);
            joinNode.addJoinChild(root);
        }
        String mCol = "";
        int maxC = -1;
        for (String key : cntCol.keySet()) {
            Integer v = cntCol.get(key);
            if (v > maxC) {
                maxC = v;
                mCol = key;
            }
        }
        joinNode.getJoinSet().add(mCol);
        return formatVerticalJoin(joinNode);
    }
    public JoinNode formatVerticalJoin(JoinNode node)
    {
        JoinNode join = new JoinNode();
        join.getJoinSet().addAll(node.getJoinSet());
        for (PlanNode p : node.getJoinChildren()) {
            if (p instanceof ProjectNode) {
                ProjectNode pn = (ProjectNode) p;
                boolean contains = true;
                for (Column c : pn.getColumns()) {
                    if (!join.getJoinSet().contains(c.getColumnName())) {
                        contains = false;
                        break;
                    }
                }
                if (contains) {
                    continue;
                }
            }
            join.addJoinChild(p);
        }
        if (join.getJoinChildren().isEmpty() && !node.getJoinChildren().isEmpty()) {
            join.getJoinChildren().add(node.getJoinChildren().get(0));
        }
        return join;
    }
    public List<String> extractColumnNameFromFilter(cn.edu.ruc.iir.pard.catalog.Table table)
    {
        String tblName = table.getTablename();
        List<String> list = new ArrayList<String>();
        if (filter.isPresent()) {
            Expr expr = Expr.parse(filter.get().getExpression());
            return Expr.extractTableColumn(expr, tblName);
        }
        return list;
    }
    public List<String> extractColumnNameFromProjection(cn.edu.ruc.iir.pard.catalog.Table table)
    {
        List<String> arrayList = new ArrayList<String>();
        for (String key : colAlias2tbl.keySet()) {
            cn.edu.ruc.iir.pard.catalog.Table t = colAlias2tbl.get(key);
            if (t.getTablename().equals(table.getTablename())) {
                String col = alias2col.get(key).getColumnName();
                arrayList.add(col);
            }
        }
        return arrayList;
    }
    public UnionNode horizonLocalization(String fromTableName)
    {
        //TableDao tdao = tableDaoMap.get(tbl2schema.get(fromTableName));
        List<String> siteList = tbl2site.get(fromTableName);
        UnionNode unionNode = new UnionNode();
        cn.edu.ruc.iir.pard.catalog.Table catalogTable = catalog.get(fromTableName);
        //union = unionNode;
        for (Fragment frag : catalogTable.getFragment().values()) {
            if (!siteList.contains(frag.getSiteName())) {
                continue;
            }
            Expr expr = Expr.parse(frag.getCondition(), fromTableName);
            PlanNode childrenNode = new TableScanNode(tbl2schema.get(fromTableName), fromTableName, frag.getSiteName());
            if (filter.isPresent()) {
                //TODO: 从expr2中选择自己的
                Expr expr2 = Expr.extractTableFilter(Expr.parse(filter.get().getExpression()), fromTableName);
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
            if (project != null) {
                //TODO: 选择自己表里元素下推
                List<String> ownList = extractColumnNameFromProjection(catalogTable);
                List<Column> singleTableProjection = new ArrayList<Column>();
                for (Column col : project.getColumns()) {
                    if (ownList.contains(col.getColumnName())) {
                        singleTableProjection.add(col);
                    }
                }
                ProjectNode pnode = new ProjectNode(singleTableProjection);
                pnode.setChildren(childrenNode, true, true);
                childrenNode = pnode;
            }
            else {
                ProjectNode pnode = new ProjectNode(new ArrayList<Column>(catalogTable.getColumns().values()));
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
    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        PardServlet.planList.add(this);
        return true;
    }
}
