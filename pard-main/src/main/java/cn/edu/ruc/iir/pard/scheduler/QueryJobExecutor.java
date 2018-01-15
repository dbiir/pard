package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.exchange.PardExchangeClient;
import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.JoinTask;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.SendDataTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.planner.Plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryJobExecutor
{
    private Job queryJob;
    private Plan plan = null;
    private List<Task> tasks = null;
    private List<SendDataTask> sendDataTask = new ArrayList<SendDataTask>();
    private List<JoinTask> joinTask = new ArrayList<JoinTask>();
    private List<Task> otherTask = new ArrayList<Task>();
    private Logger logger = Logger.getLogger(QueryJobExecutor.class.getName());
    private SiteDao siteDao = new SiteDao();
    private Map<String, Task> taskMap = null;
    private int realNodeNum = 0;
    public QueryJobExecutor(Job job)
    {
        taskMap = new HashMap<String, Task>();
        queryJob = job;
        plan = job.getPlan();
        tasks = job.getTasks();
        init();
    }
    public void init()
    {
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
        }
    }
    public void executeFirstPhase()
    {
        BlockingQueue<Block> blocks = new LinkedBlockingQueue<>();
        for (SendDataTask task : sendDataTask) {
            String site = task.getSite();
            String taskId = task.getTaskId();
            Site nodeSite = siteDao.listNodes().get(site);
            if (nodeSite == null) {
                logger.log(Level.SEVERE, "Node " + site + " is not active. Please check.");
                //return PardResultSet.execErrResultSet;
            }
            else {
                realNodeNum++;
                PardExchangeClient client = new PardExchangeClient(nodeSite.getIp(), nodeSite.getExchangePort());
                client.connect(task, blocks);
                taskMap.put(taskId, task);
            }
        }
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
            else {
                String taskId = block.getTaskId();
                taskMap.remove(taskId);
                logger.info("Task " + taskId + " done.");
            }
        }
    }

    public PardResultSet execute()
    {
        executeFirstPhase();
        PardResultSet resultSet = new PardResultSet();
        List<Task> secondList = new ArrayList<Task>();
        secondList.addAll(otherTask);
        secondList.addAll(joinTask);
        BlockingQueue<Block> blocks = new LinkedBlockingQueue<>();
        for (Task task : secondList) {
            String site = task.getSite();
            String taskId = task.getTaskId();
            Site nodeSite = siteDao.listNodes().get(site);
            if (nodeSite == null) {
                logger.log(Level.SEVERE, "Node " + site + " is not active. Please check.");
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
    }
}
