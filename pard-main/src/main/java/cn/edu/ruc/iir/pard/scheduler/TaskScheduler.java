package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.nodekeeper.Keeper;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.dml.InsertPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.server.PardStartupHook;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
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
        int siteNum = sites.size();
        List<Task> tasks = new ArrayList<>();
        if (plan instanceof SchemaCreationPlan) {
            SchemaCreationPlan schemaCreationPlan = (SchemaCreationPlan) plan;
            for (String site : sites) {
                CreateSchemaTask task = new CreateSchemaTask(
                        schemaCreationPlan.getSchemaName(),
                        schemaCreationPlan.isNotExists(),
                        site);
                tasks.add(task);
            }
        }
        if (plan instanceof TableCreationPlan) {
            TableCreationPlan tableCreationPlan = (TableCreationPlan) plan;
            if (tableCreationPlan.isAlreadyDone()) {
                return ImmutableList.of();
            }

        }
        if (plan instanceof InsertPlan) {
        }
        if (plan instanceof QueryPlan) {
        }
        return ImmutableList.copyOf(tasks);
    }

    public PardResultSet executeJob(Job job)
    {
        if (job.getJobState() != JobScheduler.JobState.EXECUTING) {
            return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
        }
        return new PardResultSet(PardResultSet.ResultStatus.OK);
    }
}
