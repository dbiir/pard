package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.exception.ErrorMessage;
import cn.edu.ruc.iir.pard.commons.exception.ParsingException;
import cn.edu.ruc.iir.pard.commons.exception.SemanticException;
import cn.edu.ruc.iir.pard.commons.exception.TaskSchedulerException;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.planner.PardPlanner;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.scheduler.Job;
import cn.edu.ruc.iir.pard.scheduler.JobScheduler;
import cn.edu.ruc.iir.pard.scheduler.TaskScheduler;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardQueryHandler
        extends Thread
{
    private Socket socket;
    private Logger logger = Logger.getLogger("pard server");
    private JobScheduler jobScheduler;
    private ObjectOutputStream objectOutputStream;
    private SqlParser sqlParser = new SqlParser();
    private PardPlanner planner = new PardPlanner();
    private TaskScheduler taskScheduler;

    public PardQueryHandler(
            Socket socket,
            JobScheduler jobScheduler,
            TaskScheduler taskScheduler)
    {
        this.socket = socket;
        this.jobScheduler = jobScheduler;
        this.taskScheduler = taskScheduler;
        try {
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        int c = 0;
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))) {
            while (true) {
                String line = input.readLine();
                if (line == null) {
                    //logger.info("Empty line");
                    c++;
                    if (c > 10) {
                        break;
                    }
                    continue;
                }
                if (line.equalsIgnoreCase("EXIT") ||
                        line.equalsIgnoreCase("QUIT")) {
                    logger.info("CLIENT QUIT");
                    break;
                }
                logger.info("QUERY: " + line);
                PardResultSet result = executeQuery(line);
                objectOutputStream.writeObject(result);
                objectOutputStream.flush();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (Exception e2) {
            e2.printStackTrace();
        }
    }

    public PardResultSet executeQuery(String sql)
    {
        logger.info("Accepted query: " + sql);
        long timerStart = System.currentTimeMillis();
        Job job = jobScheduler.newJob();
        if (job == null) {
            logger.log(Level.WARNING, "Cannot create job for sql: " + sql);
            return new PardResultSet(PardResultSet.ResultStatus.BEGIN_ERR);
        }
        job.setSql(sql);
        jobScheduler.updateJob(job.getJobId());
        logger.info("Created job for query, job id: " + job.getJobId());

        Statement statement;
        try {
            statement = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            return new PardResultSet(PardResultSet.ResultStatus.PARSING_ERR);
        }
        catch (NullPointerException e1) {
            return new PardResultSet(PardResultSet.ResultStatus.PARSING_ERR, e1.getMessage());
        }
        if (statement == null) {
            jobScheduler.failJob(job.getJobId());
            logger.log(Level.WARNING, "Cannot create statement for sql: " + sql);
            return new PardResultSet(PardResultSet.ResultStatus.PARSING_ERR);
        }
        job.setStatement(statement);
        jobScheduler.updateJob(job.getJobId());
        logger.info("Created statement for job[" + job.getJobId() + "], job state: " + job.getJobState());

        Plan plan = null;
        ErrorMessage msg = ErrorMessage.getOKMessage();
        try {
            plan = planner.plan(statement);
        }
        catch (SemanticException e) {
            logger.log(Level.WARNING, e.getSemanticErrorMessage().toString());
            msg = e.getSemanticErrorMessage();
            if (msg == null) {
                msg = ErrorMessage.getOKMessage();
            }
        }
        if (plan == null) {
            jobScheduler.failJob(job.getJobId());
            logger.log(Level.WARNING, "Cannot create plan for sql: " + sql);
            return new PardResultSet(PardResultSet.ResultStatus.PLANNING_ERR, msg.getErrmsg());
        }
        job.setPlan(plan);
        plan.setJobId(job.getJobId());
        jobScheduler.updateJob(job.getJobId());
        logger.info("Created plan for job[" + job.getJobId() + "], job state: " + job.getJobState());

        List<Task> tasks = null;
        String taskMsg = null;
        try {
            tasks = taskScheduler.generateTasks(plan);
        }
        catch (TaskSchedulerException e) {
            taskMsg = e.getPardErrorMessage().toString();
        }
        if (tasks == null) {
            jobScheduler.failJob(job.getJobId());
            logger.log(Level.WARNING, "Cannot create tasks for sql: " + sql);
            return new PardResultSet(PardResultSet.ResultStatus.SCHEDULING_ERR, taskMsg);
        }
        if (!tasks.isEmpty()) {
            tasks.forEach(job::addTask);
        }
        jobScheduler.updateJob(job.getJobId());
        logger.info("Generated tasks for job[" + job.getJobId() + "], job state: " + job.getJobState());

        PardResultSet resultSet = taskScheduler.executeJob(job);
        if (plan.getMsg() != null) {
            resultSet.setSemanticErrmsg(plan.getMsg().toString());
        }
        if (resultSet.getStatus() != PardResultSet.ResultStatus.OK) {
            jobScheduler.failJob(job.getJobId());
            logger.log(Level.WARNING, "Failed to execute job for sql: " + sql);
        }
        jobScheduler.updateJob(job.getJobId());

        long timerStop = System.currentTimeMillis();
        logger.info("Done executing job[" + job.getJobId() + "], job state: " + job.getJobState() + ", execution time: " + ((double) (timerStop - timerStart)) / 1000 + "s");
        resultSet.setExecutionTime(timerStop - timerStart);
        return resultSet;
    }
}
