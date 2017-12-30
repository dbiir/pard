package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.communication.rpc.PardRPCClient;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.nodekeeper.Keeper;
import cn.edu.ruc.iir.pard.nodekeeper.ServerInfo;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.InsertPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.server.PardStartupHook;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Row;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final Keeper nodeKeeper;
    private final SiteDao siteDao;

    private TaskScheduler()
    {
        this.nodeKeeper = Keeper.INSTANCE();
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

    public List<Task> generateTasks(Plan plan)
    {
        Set<String> sites = siteDao.listNodes();

        // use plan
        if (plan instanceof UsePlan) {
            return ImmutableList.of();
        }

        // schema creation plan
        if (plan instanceof SchemaCreationPlan) {
            List<Task> tasks = new ArrayList<>();
            SchemaCreationPlan schemaCreationPlan = (SchemaCreationPlan) plan;
            for (String site : sites) {
                CreateSchemaTask task = new CreateSchemaTask(
                        schemaCreationPlan.getSchemaName(),
                        schemaCreationPlan.isNotExists(),
                        site);
                tasks.add(task);
            }
            return ImmutableList.copyOf(tasks);
        }

        // schema drop plan
        if (plan instanceof SchemaDropPlan) {
            List<Task> tasks = new ArrayList<>();
            SchemaDropPlan schemaDropPlan = (SchemaDropPlan) plan;
            for (String site : sites) {
                DropSchemaTask task = new DropSchemaTask(schemaDropPlan.getSchemaName(),
                        schemaDropPlan.isExists(),
                        site);
                tasks.add(task);
            }
            return ImmutableList.copyOf(tasks);
        }

        // table creation plan
        if (plan instanceof TableCreationPlan) {
            List<Task> tasks = new ArrayList<>();
            TableCreationPlan tableCreationPlan = (TableCreationPlan) plan;
            if (tableCreationPlan.isAlreadyDone()) {
                return ImmutableList.of();
            }
            Map<String, Object> partitionMap = plan.getDistributionHints();
            String tableName = tableCreationPlan.getTableName();
            String schemaName = tableCreationPlan.getSchemaName();
            boolean isNotExists = tableCreationPlan.isNotExists();
            for (String site : partitionMap.keySet()) {
                List<Column> columns = (List<Column>) partitionMap.get(site);
                CreateTableTask task = new CreateTableTask(
                        schemaName,
                        tableName,
                        isNotExists,
                        columns,
                        site);
                tasks.add(task);
            }
            return ImmutableList.copyOf(tasks);
        }

        // table drop plan
        if (plan instanceof TableDropPlan) {
            List<Task> tasks = new ArrayList<>();
            return ImmutableList.copyOf(tasks);
        }

        // insert plan
        if (plan instanceof InsertPlan) {
            List<Task> tasks = new ArrayList<>();
            InsertPlan insertPlan = (InsertPlan) plan;
            Map<String, Object> partitionMap = plan.getDistributionHints();
            String tableName = insertPlan.getTableName();
            String schemaName = insertPlan.getSchemaName();
            List<Column> columns = insertPlan.getColList();
            int columnSize = columns.size();
            for (String site : partitionMap.keySet()) {
                List<Row> rows = (List<Row>) partitionMap.get(site);
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
                InsertIntoTask task = new InsertIntoTask(schemaName, tableName, columns, rowsStr);
                tasks.add(task);
            }
            return ImmutableList.copyOf(tasks);
        }

        // query plan
        if (plan instanceof QueryPlan) {
            List<Task> tasks = new ArrayList<>();
            return ImmutableList.copyOf(tasks);
        }

        return null;
    }

    public PardResultSet executeJob(Job job)
    {
        if (job.getJobState() != JobScheduler.JobState.EXECUTING) {
            return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
        }

        Plan plan = job.getPlan();
        List<Task> tasks = job.getTasks();
        List<Integer> statusL = new ArrayList<>();

        if (tasks.isEmpty()) {
            if (plan.afterExecution(true)) {
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }

        else {
            for (Task task : tasks) {
                // create schema task
                if (task instanceof CreateSchemaTask) {
                    String site = task.getSite();
                    ServerInfo info = nodeKeeper.getRpcServers().get(site);
                    if (info != null) {
                        PardRPCClient client = new PardRPCClient(info.getIp(), info.getPort());
                        int status = client.createSchema((CreateSchemaTask) task);
                        client.shutdown();
                        statusL.add(status);
                    }
                }
                // drop schema task
                if (task instanceof DropSchemaTask) {
                    String site = task.getSite();
                    ServerInfo info = nodeKeeper.getRpcServers().get(site);
                    if (info != null) {
                        PardRPCClient client = new PardRPCClient(info.getIp(), info.getPort());
                        int status = client.dropSchema((DropSchemaTask) task);
                        client.shutdown();
                        statusL.add(status);
                    }
                }
                // create table task
                if (task instanceof CreateTableTask) {
                    String site = task.getSite();
                    ServerInfo info = nodeKeeper.getRpcServers().get(site);
                    if (info != null) {
                        PardRPCClient client = new PardRPCClient(info.getIp(), info.getPort());
                        int status = client.createTable((CreateTableTask) task);
                        client.shutdown();
                        statusL.add(status);
                    }
                }
                // drop table task
                if (task instanceof DropTableTask) {
                    String site = task.getSite();
                    ServerInfo info = nodeKeeper.getRpcServers().get(site);
                    if (info != null) {
                        PardRPCClient client = new PardRPCClient(info.getIp(), info.getPort());
                        int status = client.dropTable((DropTableTask) task);
                        client.shutdown();
                        statusL.add(status);
                    }
                }
            }

            for (int status : statusL) {
                if (status <= 0) {
                    return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
                }
            }
        }
        return new PardResultSet(PardResultSet.ResultStatus.OK);
    }
}
