package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.commons.exception.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.commons.exception.TaskSchedulerException;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import cn.edu.ruc.iir.pard.communication.rpc.PardRPCClient;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.exchange.PardExchangeClient;
import cn.edu.ruc.iir.pard.exchange.PardFileExchangeClient;
import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DeleteTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.JoinTask;
import cn.edu.ruc.iir.pard.executor.connector.LoadTask;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.QueryTask;
import cn.edu.ruc.iir.pard.executor.connector.SendDataTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.JoinNode;
import cn.edu.ruc.iir.pard.executor.connector.node.NodeHelper;
import cn.edu.ruc.iir.pard.executor.connector.node.OutputNode;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.ProjectNode;
import cn.edu.ruc.iir.pard.executor.connector.node.TableScanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.UnionNode;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaShowPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableShowPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.DeletePlan;
import cn.edu.ruc.iir.pard.planner.dml.InsertPlan;
import cn.edu.ruc.iir.pard.planner.dml.LoadPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.server.PardStartupHook;
import cn.edu.ruc.iir.pard.sql.expr.ColumnItem;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.SingleExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Row;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * pard task scheduler.
 * server level.
 *
 * This is just a simple execution controller for job.
 * Cannot do scheduling at all ...
 *
 * @author guodong
 */
public class TaskScheduler
        implements PardStartupHook
{
    private final Logger logger = Logger.getLogger(TaskScheduler.class.getName());
    private final SiteDao siteDao;

    private TaskScheduler()
    {
        this.siteDao = new SiteDao();
    }

    @Override
    public void startup() throws RuntimeException
    {
        this.INSTANCE();
    }

    private static final class TaskSchedulerHolder
    {
        private static final TaskScheduler instance = new TaskScheduler();
    }

    public static final TaskScheduler INSTANCE()
    {
        return TaskSchedulerHolder.instance;
    }

    // todo this sucks, full of if else
    public List<Task> generateTasks(Plan plan)
    {
        Set<String> sites = siteDao.listNodes().keySet();

        // use plan
        if (plan instanceof UsePlan) {
            logger.info("Task generation for use plan");
            return ImmutableList.of();
        }

        // schema creation plan
        if (plan instanceof SchemaCreationPlan) {
            logger.info("Task generation for schema creation plan");
            List<Task> tasks = new ArrayList<>();
            SchemaCreationPlan schemaCreationPlan = (SchemaCreationPlan) plan;
            int index = 0;
            for (String site : sites) {
                CreateSchemaTask task = new CreateSchemaTask(
                        schemaCreationPlan.getSchemaName(),
                        schemaCreationPlan.isNotExists(),
                        site);
                task.setTaskId(plan.getJobId() + "-" + index);
                tasks.add(task);
                index++;
            }
            return ImmutableList.copyOf(tasks);
        }

        // schema drop plan
        if (plan instanceof SchemaDropPlan) {
            logger.info("Task generation for schema drop plan");
            List<Task> tasks = new ArrayList<>();
            SchemaDropPlan schemaDropPlan = (SchemaDropPlan) plan;
            int index = 0;
            for (String site : sites) {
                DropSchemaTask task = new DropSchemaTask(schemaDropPlan.getSchemaName(),
                        schemaDropPlan.isExists(),
                        site);
                task.setTaskId(plan.getJobId() + "-" + index);
                tasks.add(task);
                index++;
            }
            return ImmutableList.copyOf(tasks);
        }

        // table creation plan
        if (plan instanceof TableCreationPlan) {
            logger.info("Task generation for table creation plan");
            List<Task> tasks = new ArrayList<>();
            TableCreationPlan tableCreationPlan = (TableCreationPlan) plan;
            if (tableCreationPlan.isAlreadyDone()) {
                return ImmutableList.of();
            }
            Map<String, List<Column>> partitionMap = tableCreationPlan.getDistributionHints();
            String tableName = tableCreationPlan.getTableName();
            String schemaName = tableCreationPlan.getSchemaName();
            boolean isNotExists = tableCreationPlan.isNotExists();
            int index = 0;
            for (String site : partitionMap.keySet()) {
                List<Column> columns = partitionMap.get(site);
                CreateTableTask task = new CreateTableTask(
                        schemaName,
                        tableName,
                        isNotExists,
                        columns,
                        site);
                task.setTaskId(plan.getJobId() + "-" + index);
                tasks.add(task);
                index++;
            }
            return ImmutableList.copyOf(tasks);
        }

        // table drop plan
        if (plan instanceof TableDropPlan) {
            logger.info("Task generation for table drop plan");
            List<Task> tasks = new ArrayList<>();
            TableDropPlan tableDropPlan = (TableDropPlan) plan;
            String schemaName = tableDropPlan.getSchemaName();
            String tableName = tableDropPlan.getTableName();
            Set<String> siteNames = tableDropPlan.getDistributionHints().keySet();
            int index = 0;
            for (String sn : siteNames) {
                Task task = new DropTableTask(schemaName, tableName, sn);
                task.setTaskId(plan.getJobId() + "-" + index);
                tasks.add(task);
                index++;
            }
            return ImmutableList.copyOf(tasks);
        }

        // show schemas
        if (plan instanceof SchemaShowPlan) {
            return ImmutableList.of();
        }

        // show tables
        if (plan instanceof TableShowPlan) {
            return ImmutableList.of();
        }

        // load
        if (plan instanceof LoadPlan) {
            LoadPlan loadPlan = (LoadPlan) plan;
            String schemaName = loadPlan.getSchemaName();
            String tableName = loadPlan.getTableName();
            Map<String, List<String>> distributionHints = ((LoadPlan) plan).getDistributionHints();
            List<Task> tasks = new ArrayList<>();
            int index = 0;
            for (String site : distributionHints.keySet()) {
                Task task = new LoadTask(schemaName, tableName, distributionHints.get(site), site);
                task.setTaskId(plan.getJobId() + "-" + index);
                tasks.add(task);
                index++;
            }
            return ImmutableList.copyOf(tasks);
        }

        // insert plan
        if (plan instanceof InsertPlan) {
            logger.info("Task generation for insert plan");
            List<Task> tasks = new ArrayList<>();
            InsertPlan insertPlan = (InsertPlan) plan;
            Map<String, List<Row>> partitionMap = insertPlan.getDistributionHints();
            String tableName = insertPlan.getTableName();
            String schemaName = insertPlan.getSchemaName();
            int index = 0;
            for (String site : partitionMap.keySet()) {
                List<Column> columns = insertPlan.getColListMap().get(site);
                int columnSize = columns.size();
                List<Row> rows = partitionMap.get(site);
                int rowSize = rows.size();
                String[][] rowsStr = new String[rowSize][];
                int rowIndex = 0;
                for (Row row : rows) {
                    String[] rowStr = new String[columnSize];
                    int colIndex = 0;
                    for (Expression expression : row.getItems()) {
                        rowStr[colIndex] = expression.toString();
                        colIndex++;
                    }
                    rowsStr[rowIndex] = rowStr;
                    rowIndex++;
                }
                InsertIntoTask task = new InsertIntoTask(schemaName, tableName, columns, rowsStr, site);
                task.setTaskId(plan.getJobId() + "-" + index);
                tasks.add(task);
                index++;
            }
            return ImmutableList.copyOf(tasks);
        }

        if (plan instanceof DeletePlan) {
            logger.info("Task generation for delete plan");
            DeletePlan deletePlan = (DeletePlan) plan;
            Map<String, Expr> distributionHints = deletePlan.getDistributionHints();
            List<Task> tasks = new ArrayList<>();
            int index = 0;
            for (String site : distributionHints.keySet()) {
                DeleteTask task = new DeleteTask(
                        deletePlan.getSchemaName(),
                        deletePlan.getTableName(),
                        distributionHints.get(site).toExpression(),
                        site);
                task.setTaskId(plan.getJobId() + "-" + index);
                tasks.add(task);
                index++;
            }
            return ImmutableList.copyOf(tasks);
        }

        // query plan
        if (plan instanceof QueryPlan) {
            QueryPlan queryPlan = (QueryPlan) plan;
            try {
                return processQueryPlan2(queryPlan);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public List<Task> processQueryPlan2(QueryPlan queryPlan)
    {
        logger.info("Task generation for query plan");
        PlanNode planNode = queryPlan.getPlan();
        ProjectNode proj = null;
        PlanNode currentNode = planNode;
        UnionNode internalUnionNode = null;
        while (currentNode != null) {
            if (currentNode instanceof UnionNode) {
                internalUnionNode = (UnionNode) currentNode;
            }
            if (currentNode instanceof ProjectNode) {
                proj = (ProjectNode) currentNode;
            }
            currentNode = currentNode.getLeftChild();
        }
        if (internalUnionNode == null) {
            return ImmutableList.of(new QueryTask(planNode));
        }
        else {
            return ImmutableList.copyOf(processUnionTask(internalUnionNode, queryPlan.getJobId(), new AtomicInteger(1), proj));
        }
    }
    public QueryTask singleSiteTableTask(PlanNode node, String jobId, AtomicInteger jobOffset)
    {
        TableScanNode tableScanNode = null;
        if (node instanceof TableScanNode) {
            tableScanNode = (TableScanNode) node;
        }
        while (!(node instanceof TableScanNode) && node.hasChildren()) {
            if (node.getLeftChild() instanceof TableScanNode) {
                tableScanNode = (TableScanNode) node.getLeftChild();
                break;
            }
            node = node.getLeftChild();
        }
        if (tableScanNode == null) {
            return null;
        }
        QueryTask task = new QueryTask(tableScanNode.getSite(), NodeHelper.copyNode(node));
        task.setTaskId(jobId + "-" + jobOffset.addAndGet(1));
        return task;
    }
    public static class Pointer<T>
    {
        private T value;

        public Pointer(T value)
        {
            super();
            this.value = value;
        }
        public T getValue()
        {
            return value;
        }

        public void setValue(T value)
        {
            this.value = value;
        }
        public boolean isEmpty()
        {
            return value == null;
        }
    }
    public TableScanNode getTableScanNode(PlanNode node)
    {
        while (node != null) {
            if (node instanceof TableScanNode) {
                return (TableScanNode) node;
            }
            node = node.getLeftChild();
        }
        return null;
    }
    public Expr getTableFilterNode(PlanNode node)
    {
        while (node != null) {
            if (node instanceof FilterNode) {
                return Expr.parse(((FilterNode) node).getExpression());
            }
            node = node.getLeftChild();
        }
        return new TrueExpr();
    }
    //TODO: add projection.
    public void processJoinTask(JoinNode node, Map<String, JoinTask> joinMap, Map<String, SendDataTask> sendDataMap, List<Task> otherTask, Pointer<String> joinTableName, Pointer<String> dataTableName, String jobId, AtomicInteger jobOffset, ProjectNode proj)
    {
        PlanNode joinNode = null;
        PlanNode dataNode = null;
        TableScanNode joinTable = null;
        TableScanNode dataTable = null;
        Expr joinExpr = null;
        Expr dataExpr = null;

        PlanNode left = node.getJoinChildren().get(0);
        //System.out.println("left" + left);
        PlanNode right = node.getJoinChildren().get(1);
        //System.out.println("right" + right);
        TableScanNode leftTable = getTableScanNode(left);
        TableScanNode rightTable = getTableScanNode(right);
        if (leftTable == null || rightTable == null) {
            throw new TaskSchedulerException(ErrCode.UnSupportedQuery, " two or more table participate one join");
        }
        if (joinTableName.isEmpty() || dataTableName.isEmpty()) {
            joinTableName.setValue(leftTable.getTable());
            dataTableName.setValue(rightTable.getTable());
        }
        if (leftTable.getTable().equals(joinTableName.getValue()) && rightTable.getTable().equals(dataTableName.getValue())) {
            joinNode = left;
            dataNode = right;
            joinTable = leftTable;
            dataTable = rightTable;
        }
        else if ((rightTable.getTable().equals(joinTableName.getValue()) && leftTable.getTable().equals(dataTableName.getValue()))) {
            joinNode = right;
            dataNode = left;
            joinTable = rightTable;
            dataTable = leftTable;
        }
        else {
            throw new TaskSchedulerException(ErrCode.UnSupportedQuery, "below one union has more than one group of joins.");
        }
        System.out.println("joinNode1" + joinNode);
        joinExpr = getTableFilterNode(joinNode);
        dataExpr = getTableFilterNode(dataNode);
        SendDataTask dataTask = sendDataMap.get(dataTable.getSite());

        Expr dataTaskExpr = extractTableExpr(joinExpr, dataExpr, dataTableName.getValue(), node);
        Expr joinTaskSingleTableExpr = extractTableExpr(joinExpr, dataExpr, joinTableName.getValue(), node);
        String tmpTableName = "tmp_" + dataTableName.getValue() + "_" + jobId + "_" + joinTable.getSite();
        tmpTableName = tmpTableName.replace('-', '_').replace('-', '_').replace('-', '_');
        tmpTableName += (int) (Math.random() * Integer.MAX_VALUE);
        while (tmpTableName.contains(" ")) {
            tmpTableName = tmpTableName.replace(" ", "");
        }
        boolean needDropTable = true;
//        if (dataTable.getSite().equals(joinTable.getSite())) {
            //位于相同站点，不需要发数据
//            tmpTableName = dataTableName.getValue();
//            needDropTable = false;
 //       }
//        else {
        if (dataTask == null) {
            dataTask = new SendDataTask(dataTable.getSite());
            dataTask.setSchemaName(dataTable.getTable());
            PlanNode p = new OutputNode();
            p.setChildren(NodeHelper.copyNode(dataNode), true, true);
            dataTask.setNode(orFilterNode(p, dataTaskExpr));
            dataTask.getSiteExpression().put(joinTable.getSite(), dataTaskExpr.toExpression());
            dataTask.setTaskId(jobId + "_" + jobOffset.addAndGet(1));
            dataTask.getTmpTableMap().put(joinTable.getSite(), tmpTableName);
            sendDataMap.put(dataTable.getSite(), dataTask);
        }
        else {
            PlanNode p = dataTask.getNode();
            dataTask.setNode(orFilterNode(p, dataTaskExpr));
            dataTask.getSiteExpression().put(joinTable.getSite(), dataTaskExpr.toExpression());
            dataTask.getTmpTableMap().put(joinTable.getSite(), tmpTableName);
        }
//        }
        JoinTask joinTask = joinMap.get(joinTable.getSite());
        if (joinTask == null) {
            //Do sth.
            List<Column> clist = new ArrayList<Column>();
            for (Column c : proj.getColumns()) {
                Column col = new Column(c);
                if (dataTableName.getValue().equals(col.getTableName())) {
                    col.setTableName(tmpTableName);
                }
                clist.add(col);
            }
            PlanNode output = new OutputNode();
            PlanNode p = new ProjectNode(clist);
            output.setChildren(p, true, true);
            JoinNode join = new JoinNode();
            p.setChildren(join, true, true);
            join.getJoinSet().addAll(node.getJoinSet());
            join.getExprList().addAll(node.getExprList());
            //TODO: add children.
            join.addJoinChild(NodeHelper.copyNode(joinNode));
            //System.out.println("joinNode1" + joinNode);
            //join.addJoinChild(orFilterNode(NodeHelper.copyNode(joinNode), joinTaskSingleTableExpr, dataTableName.getValue(), tmpTableName));
            PlanNode dataCopy = NodeHelper.copyNode(dataNode);
            PlanNode tmp = dataCopy;
            if (dataCopy instanceof TableScanNode) {
                TableScanNode scan = (TableScanNode) dataCopy;
                dataCopy = new TableScanNode(scan.getSchema(), tmpTableName, joinTable.getSite());
                ((TableScanNode) dataCopy).setAlias(dataTableName.getValue());
            }
            else {
                while (tmp.hasChildren()) {
                    if (tmp.getLeftChild() instanceof TableScanNode) {
                        TableScanNode scan = (TableScanNode) tmp.getLeftChild();
                        scan = new TableScanNode(scan.getSchema(), tmpTableName, joinTable.getSite());
                        scan.setAlias(dataTableName.getValue());
                        tmp.setChildren(scan, true, false);
                        break;
                    }
                    tmp = tmp.getLeftChild();
                }
            }
            join.addJoinChild(dataCopy);
            joinTask = new JoinTask(joinTable.getSite());
            if (needDropTable) {
                joinTask.setTmpTableName(tmpTableName);
            }
            else {
                joinTask.setTmpTableName(null);
            }
            joinTask.setTaskId(jobId + "_" + jobOffset.addAndGet(1));
            joinTask.setNode(output);
            //System.out.println("joinNode1" + p);
            joinMap.put(joinTable.getSite(), joinTask);
        }
        else {
            PlanNode p = joinTask.getNode();
            p = NodeHelper.copyNode(p);
            PlanNode root = p;
            while (p.hasChildren()) {
                if (p.getLeftChild() instanceof JoinNode) {
                    JoinNode jnode = (JoinNode) p.getLeftChild();
                    //System.out.println();
                    //System.out.println(jnode.getJoinChildren().size() + "aaaaaaaaa");
                    PlanNode pnode = jnode.getJoinChildren().get(0);
                    pnode = orFilterNode(NodeHelper.copyNode(pnode), joinTaskSingleTableExpr);
                    jnode.getJoinChildren().set(0, pnode);
                    break;
                }
                p = p.getLeftChild();
            }
            joinTask.setNode(root);
        }
    }
    public PlanNode orFilterNode(PlanNode node, Expr filterExpr)
    {
        //System.out.println(node == null);
        node = NodeHelper.copyNode(node);
        //System.out.println(node == null);
        PlanNode root = node;
        while (node.hasChildren()) {
            if (node.getLeftChild() instanceof FilterNode) {
                FilterNode children = (FilterNode) node.getLeftChild();
                Expr e1 = Expr.parse(children.getExpression());
                //if (oldTableName != null) {
                    //System.out.println("before replace e1 " + e1.toString() + "from " + oldTableName + " to" + newTableName);
                    //e1 = Expr.replaceTableName(e1);
                    //filterExpr = Expr.replaceTableName(filterExpr, oldTableName, newTableName);
                    //System.out.println("after replace e1 " + e1.toString());
                //}
                e1 = Expr.or(e1, filterExpr, LogicOperator.AND);
                children.setExpression(e1.toExpression());
                return root;
            }
            node = node.getLeftChild();
            if (node == null) {
                break;
            }
        }
        return root;
    }
    public Expr extractTableExpr(Expr joinExpr, Expr dataExpr, String tableName, JoinNode node)
    {
        Expr e = Expr.and(joinExpr, dataExpr, LogicOperator.AND);
        if (node.getExprList().size() == 0) {
            return new TrueExpr();
        }
        SingleExpr se = (SingleExpr) Expr.parse(node.getExprList().get(0));
        ColumnItem lv = (ColumnItem) se.getLvalue();
        ColumnItem rv = (ColumnItem) se.getRvalue();
        if (lv.getTableName().equalsIgnoreCase(tableName)) {
            Expr opt = Expr.replace(e, rv, lv);
            opt = Expr.extractTableFilter(opt, tableName);
            return Expr.optimize(opt, LogicOperator.AND);
        }
        else if (rv.getTableName().equalsIgnoreCase(tableName)) {
            Expr opt = Expr.replace(e, lv, rv);
            opt = Expr.extractTableFilter(opt, tableName);
            return Expr.optimize(opt, LogicOperator.AND);
        }
        else {
            throw new TaskSchedulerException(ErrCode.ParseError, "expression that can replace");
        }
    }
    public List<Task> processUnionTask(UnionNode union, String jobId, AtomicInteger jobOffset, ProjectNode proj)
    {
        List<Task> tasks = new ArrayList<>();
        List<PlanNode> unionChildren = union.getUnionChildren();
        //List<SendDataTask> sendDataList = new ArrayList<SendDataTask>();
        //List<JoinTask> joinList = new ArrayList<JoinTask>();
        List<Task> otherTask = new ArrayList<Task>();
        //int index = jobOffset;
        Map<String, SendDataTask> sendDataMap = new HashMap<String, SendDataTask>();
        Map<String, JoinTask> joinMap = new HashMap<String, JoinTask>();
        Pointer<String> joinTableName = new Pointer<String>(null);
        Pointer<String> dataTableName = new Pointer<String>(null);
        for (PlanNode childNode : unionChildren) {
            //union.setChildren(childNode, true, false);
            PlanNode node = childNode;
            while (node != null) {
                if (node instanceof TableScanNode) {
                    PlanNode p = new OutputNode();
                    PlanNode root = p;
                    if (!(childNode instanceof ProjectNode)) {
                        PlanNode projection = NodeHelper.copyNode(proj);
                        if (projection != null) {
                            p.setChildren(projection, true, true);
                            p = projection;
                        }
                    }
                    p.setChildren(childNode, true, true);
                    root = NodeHelper.copyNode(root);
                    Task tableTask = singleSiteTableTask(root, jobId, jobOffset);
                    if (tableTask != null) {
                        otherTask.add(tableTask);
                    }
                    break;
                }
                else if (node instanceof JoinNode && ((JoinNode) node).getJoinChildren().size() == 2) {
                    //需要收集task的site,确定主表从表
                    processJoinTask((JoinNode) node, joinMap, sendDataMap, otherTask, joinTableName, dataTableName, jobId, jobOffset, proj);
                }
                else if (node instanceof JoinNode && ((JoinNode) node).getJoinChildren().size() == 1) {
                    node = ((JoinNode) node).getJoinChildren().get(0);
                    //don't need to do anything.
                }
                else if (node instanceof JoinNode) {
                    throw new TaskSchedulerException(ErrCode.UnSupportedQuery, " join node has more than one children");
                }
                node = node.getLeftChild();
            }
        }
        tasks.addAll(otherTask);
        tasks.addAll(sendDataMap.values());
       // tasks.addAll(joinMap.values());
        tasks.addAll(joinMap.values());
        //joinMap.values().forEach(tasks::add);
        return tasks;
    }
    public List<Task> processQueryPlan(QueryPlan queryPlan)
    {
        logger.info("Task generation for query plan");
        PlanNode planNode = queryPlan.getPlan();
        PlanNode currentNode = planNode;
        UnionNode internalUnionNode = null;
        while (currentNode.hasChildren()) {
            currentNode = currentNode.getLeftChild();
            if (currentNode instanceof UnionNode) {
                internalUnionNode = (UnionNode) currentNode;
                break;
            }
        }
        if (internalUnionNode == null) {
            return ImmutableList.of(new QueryTask(planNode));
        }
        List<Task> tasks = new ArrayList<>();
        List<PlanNode> unionChildren = internalUnionNode.getUnionChildren();
        int index = 0;
        for (PlanNode childNode : unionChildren) {
            internalUnionNode.setChildren(childNode, true, false);
            PlanNode node = childNode;
            TableScanNode tableScanNode = null;
            if (node instanceof TableScanNode) {
                tableScanNode = (TableScanNode) node;
            }
            while (!(node instanceof TableScanNode) && node.hasChildren()) {
                if (node.getLeftChild() instanceof TableScanNode) {
                    tableScanNode = (TableScanNode) node.getLeftChild();
                    break;
                }
                node = node.getLeftChild();
            }
            if (tableScanNode == null) {
                return null;
            }
            QueryTask task = new QueryTask(tableScanNode.getSite(), NodeHelper.copyNode(planNode));
            task.setTaskId(queryPlan.getJobId() + "-" + index);
            tasks.add(task);
            index++;
        }
        return ImmutableList.copyOf(tasks);
    }
    public TaskState executeQueryTask(List<Task> tasks, boolean isLocal)
    {
        if (isLocal) {
            PardResultSet resultSet = new PardResultSet();
            Map<String, Task> taskMap = new HashMap<>();
            BlockingQueue<Block> blocks = new LinkedBlockingQueue<>();
            TaskState state = new TaskState(taskMap, blocks);
            state.setResultSet(resultSet);
            for (Task task : tasks) {
                String site = task.getSite();
                String taskId = task.getTaskId();
                Site nodeSite = siteDao.listNodes().get(site);
                if (nodeSite == null) {
                    logger.log(Level.SEVERE, "Node " + site + " is not active. Please check.");
                    state.setResultSet(PardResultSet.execErrResultSet);
                    return state;
                }
                PardExchangeClient client = new PardExchangeClient(nodeSite.getIp(), nodeSite.getExchangePort());
                client.connect(task, blocks);
                taskMap.put(taskId, task);
            }
            return state;
        }
        return null;
    }
    /*
    public PardResultSet executeQueryPlanJob(Job job, QueryPlan plan, List<Task> tasks)
    {
        logger.info("Executing query tasks for job[" + job.getJobId() + "]");
        PardResultSet resultSet = new PardResultSet();
        Map<String, Task> taskMap = new HashMap<>();
        BlockingQueue<Block> blocks = new LinkedBlockingQueue<>();
        List<SendDataTask> sendDataTask = new ArrayList<SendDataTask>();
        List<JoinTask> joinTask = new ArrayList<JoinTask>();
        List<Task> otherTask = new ArrayList<Task>();
        for (Task task : tasks) {
            if (task instanceof SendDataTask) {
                sendDataTask.add((SendDataTask) task);
                continue;
            }
            if (task instanceof JoinTask) {
                joinTask.add((JoinTask) task);
                continue;
            }
            else {
                otherTask.add(task);
            }
            String site = task.getSite();
            String taskId = task.getTaskId();
            Site nodeSite = siteDao.listNodes().get(site);
            if (nodeSite == null) {
                logger.log(Level.SEVERE, "Node " + site + " is not active. Please check.");
                return PardResultSet.execErrResultSet;
            }
            PardExchangeClient client = new PardExchangeClient(nodeSite.getIp(), nodeSite.getExchangePort());
            client.connect(task, blocks);
            taskMap.put(taskId, task);
        }
        // wait for all tasks done
        while (!taskMap.isEmpty()) {
            Block block = null;
            try {
                block = blocks.poll(8000, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (block == null) {
                logger.info("Waiting for more blocks...");
                continue;
            }
            resultSet.addBlock(block);
            logger.info("Added block " + block.getSequenceId() + ", num of rows: " + block.getRows().size());
            if (!block.isSequenceHasNext()) {
                String taskId = block.getTaskId();
                taskMap.remove(taskId);
                logger.info("Task " + taskId + " done.");
            }
        }
        plan.afterExecution(true);
        return resultSet;
    }*/
    // todo this sucks, full of if else
    public PardResultSet executeJob(Job job)
    {
        logger.info("Executing job[" + job.getJobId() + "]");
        SiteDao siteDao = new SiteDao();

        if (job.getJobState() != JobScheduler.JobState.EXECUTING) {
            logger.log(Level.WARNING, "Job[" + job.getJobId() + "] is in not in executing state");
            return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
        }

        Plan plan = job.getPlan();
        List<Task> tasks = job.getTasks();

        // job does not need remote tasks
        if (tasks.isEmpty()) {
            logger.info("Job[" + job.getJobId() + "] has empty task list");
            // show schemas
            if (plan instanceof SchemaShowPlan) {
                SchemaDao schemaDao = new SchemaDao();
                Set<String> schemas = schemaDao.listAll();
                Column header = new Column(0, DataType.VARCHAR.getType(), "schema", 100, 0, 0, null);
                PardResultSet resultSet = new PardResultSet(PardResultSet.ResultStatus.OK, ImmutableList.of(header));
                for (String schemaName : schemas) {
                    RowConstructor rowConstructor = new RowConstructor();
                    rowConstructor.appendString(schemaName);
                    resultSet.add(rowConstructor.build());
                }
                return resultSet;
            }
            // show tables
            if (plan instanceof TableShowPlan) {
                SchemaDao schemaDao = new SchemaDao();
                Schema schema = schemaDao.loadByName(((TableShowPlan) plan).getSchema());
                List<Table> tables = schema.getTableList();
                Column header = new Column(0, DataType.VARCHAR.getType(), "table", 100, 0, 0, null);
                PardResultSet resultSet = new PardResultSet(PardResultSet.ResultStatus.OK, ImmutableList.of(header));
                for (Table table : tables) {
                    RowConstructor rowConstructor = new RowConstructor();
                    rowConstructor.appendString(table.getTablename());
                    resultSet.add(rowConstructor.build());
                }
                return resultSet;
            }
            // I don't know who will come here currently, just keep it
            if (plan.afterExecution(true)) {
                return PardResultSet.okResultSet;
            }
        }
        else {
            // load
            if (plan instanceof LoadPlan) {
                Map<String, Task> taskMap = new HashMap<>();
                ConcurrentLinkedQueue<PardResultSet> resultSets = new ConcurrentLinkedQueue<>();
                // distribute file
                for (Task task : tasks) {
                    String site = task.getSite();
                    Site nodeSite = siteDao.listNodes().get(site);
                    if (nodeSite == null) {
                        logger.info("No corresponding node " + site + " found for execution.");
                        return PardResultSet.execErrResultSet;
                    }
                    LoadTask loadTask = (LoadTask) task;
                    List<String> loadPaths = loadTask.getPaths();
                    if (loadPaths.isEmpty()) {
                        logger.info("No path found for execution");
                        return PardResultSet.execErrResultSet;
                    }
                    PardFileExchangeClient exchangeClient = new PardFileExchangeClient(
                                    nodeSite.getIp(),
                                    nodeSite.getFileExchangePort(),
                            loadPaths.get(0),
                                    ((LoadPlan) plan).getSchemaName(),
                                    ((LoadPlan) plan).getTableName(),
                                    task.getTaskId(),
                                    resultSets);
                    exchangeClient.run();
                    taskMap.put(task.getTaskId(), task);
                }
                while (!taskMap.isEmpty()) {
                    PardResultSet resultSet = resultSets.poll();
                    if (resultSet == null) {
                        continue;
                    }
                    taskMap.remove(resultSet.getTaskId());
                    if (resultSet.getStatus() != PardResultSet.ResultStatus.OK) {
                        return PardResultSet.execErrResultSet;
                    }
                }
                return PardResultSet.okResultSet;
            }

            // distribute query result and collect
            // this is a simplest implementation
            // todo collected result set form exchange client shall be passed on for next query stage
            if (plan instanceof QueryPlan) {
                return new QueryJobExecutor(job).execute();
            }

            // delete
            if (plan instanceof DeletePlan) {
                Map<String, Task> taskMap = new HashMap<>();
                BlockingQueue<Block> blocks = new LinkedBlockingDeque<>(100);
                for (Task task : tasks) {
                    String site = task.getSite();
                    Site nodeSite = siteDao.listNodes().get(site);
                    if (nodeSite == null) {
                        logger.info("No corresponding node " + site + " found for execution.");
                        return PardResultSet.execErrResultSet;
                    }
                    PardExchangeClient client = new PardExchangeClient(nodeSite.getIp(), nodeSite.getExchangePort());
                    client.connect(task, blocks);
                    taskMap.put(task.getTaskId(), task);
                }
                // wait for all tasks done
                while (!taskMap.isEmpty()) {
                    Block block = null;
                    try {
                        block = blocks.poll(8000, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (block == null) {
                        logger.info("Waiting for more blocks...");
                        continue;
                    }
                    if (!block.isSequenceHasNext()) {
                        String taskId = block.getTaskId();
                        taskMap.remove(taskId);
                        logger.info("Task " + taskId + " done.");
                    }
                }
                return PardResultSet.okResultSet;
            }

            // rpc task
            else {
                List<Integer> statusL = new ArrayList<>();
                for (Task task : tasks) {
                    String site = task.getSite();
                    Site nodeSite = siteDao.listNodes().get(site);
                    if (nodeSite == null) {
                        logger.info("No corresponding node " + site + " found for execution.");
                        return PardResultSet.execErrResultSet;
                    }
                    PardRPCClient client = new PardRPCClient(nodeSite.getIp(), nodeSite.getRpcPort());
                    // create schema task
                    if (task instanceof CreateSchemaTask) {
                        logger.info("Calling schema creation");
                        int status = client.createSchema((CreateSchemaTask) task);
                        statusL.add(status);
                        client.shutdown();
                    }
                    // drop schema task
                    if (task instanceof DropSchemaTask) {
                        logger.info("Calling schema drop");
                        int status = client.dropSchema((DropSchemaTask) task);
                        statusL.add(status);
                        client.shutdown();
                    }
                    // create table task
                    if (task instanceof CreateTableTask) {
                        logger.info("Calling table creation");
                        int status = client.createTable((CreateTableTask) task);
                        statusL.add(status);
                        client.shutdown();
                    }
                    // drop table task
                    if (task instanceof DropTableTask) {
                        logger.info("Calling task drop");
                        int status = client.dropTable((DropTableTask) task);
                        statusL.add(status);
                        client.shutdown();
                    }
                    // insert task
                    if (task instanceof InsertIntoTask) {
                        logger.info("Calling insert");
                        int status = client.insertInto((InsertIntoTask) task);
                        statusL.add(status);
                        client.shutdown();
                    }
                }
                for (int status : statusL) {
                    if (status <= 0) {
                        logger.info("Check task execution status. Wrong status " + status + " found.");
                        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
                    }
                }
            }
            if (!plan.afterExecution(true)) {
                logger.info("After execution failed!");
                return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
            }
        }
        return new PardResultSet(PardResultSet.ResultStatus.OK);
    }
}
