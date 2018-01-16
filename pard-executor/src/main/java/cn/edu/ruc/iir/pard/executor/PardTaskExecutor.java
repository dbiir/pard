package cn.edu.ruc.iir.pard.executor;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import cn.edu.ruc.iir.pard.etcd.dao.SiteDao;
import cn.edu.ruc.iir.pard.exchange.PardFileExchangeClient;
import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.QueryTask;
import cn.edu.ruc.iir.pard.executor.connector.SendDataTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardTaskExecutor
{
    private final Logger logger = Logger.getLogger(PardTaskExecutor.class.getName());
    private final Map<String, PardResultSet> resultSetMap;    // taskId -> result set
    private final Map<String, List<Column>> schemaMap;        // taskId -> schema
    private final Map<String, Integer> sequenceIds;           // taskId -> sequence id
    private Connector connector;

    private PardTaskExecutor()
    {
        this.resultSetMap = new HashMap<>();
        this.schemaMap = new HashMap<>();
        this.sequenceIds = new HashMap<>();
    }

    private static class PardTaskExecutorHolder
    {
        private static final PardTaskExecutor instance = new PardTaskExecutor();
    }

    public static final PardTaskExecutor INSTANCE()
    {
        return PardTaskExecutorHolder.instance;
    }

    public void setConnector(Connector connector)
    {
        this.connector = connector;
    }

    public PardResultSet executeStatus(Task task)
    {
        return connector.execute(task);
    }

    public Block executeQuery(Task task)
    {
        if (task instanceof QueryTask) {
            return executeSelect(task);
        }
        if (task instanceof SendDataTask) {
            return executeSendData(task);
        }
        else { // task instanceof JoinTask
            return executeJoin(task);
        }
    }

    private Block executeSendData(Task task)
    {
        //Map<String, Task> taskMap = new HashMap<>();
        SiteDao siteDao = new SiteDao();
        String taskId = task.getTaskId();
        logger.info("Executing SendDataTask " + taskId);
        if (!resultSetMap.containsKey(taskId)) {
            PardResultSet pardResultSet = connector.execute(task);
            resultSetMap.put(taskId, pardResultSet);
            sequenceIds.put(taskId, 0);
        }

        PardResultSet resultSet = resultSetMap.get(taskId);
        int seq = sequenceIds.get(taskId) + 1;
        List<Column> column = new ArrayList<>();
        Column col0 = new Column();
        col0.setDataType(DataType.INT.getType());
        col0.setColumnName("id");
        column.add(col0);
        Block block = new Block(column, 50 * 1024 * 1024, seq, taskId);
        block.setSequenceHasNext(false);
        sequenceIds.put(taskId, seq);
        if (resultSet.getStatus() == PardResultSet.ResultStatus.OK) {
            for (Map.Entry<String, String> entry : ((SendDataTask) task).getTmpTableMap().entrySet()) {
                Site nodeSite = siteDao.listNodes().get(entry.getKey());

                File file = new File("/dev/shm/" + entry.getKey() +
                        ((SendDataTask) task).getTmpTableMap().get(entry.getKey()) +
                        "SENDDATA");

                if (file.exists() && fileLength(file) > 2) {
                    ConcurrentLinkedQueue<PardResultSet> resultSets = new ConcurrentLinkedQueue<>();
                    PardFileExchangeClient pfec = new PardFileExchangeClient(nodeSite.getIp(),
                            nodeSite.getFileExchangePort(),
                            file.getPath(),
                            ((SendDataTask) task).getSchemaName(),
                            ((SendDataTask) task).getTmpTableMap().get(entry.getKey()),
                            task.getTaskId(),
                            resultSets);
                    pfec.run();
                    //taskMap.put(task.getTaskId(), task);
                }
            }
            return block;
        }
        else {
            RowConstructor rowConstructor = new RowConstructor();
            rowConstructor.appendInt(0);
            block.setSequenceHasNext(true);
            block.addRow(rowConstructor.build());
            return block;
        }
    }

    private Block executeJoin(Task task)
    {
        String taskId = task.getTaskId();
        logger.info("Executing JoinTask " + taskId);
        if (!resultSetMap.containsKey(taskId)) {
            PardResultSet pardResultSet = connector.execute(task);
            resultSetMap.put(taskId, pardResultSet);
            sequenceIds.put(taskId, 0);
        }
        PardResultSet resultSet = resultSetMap.get(taskId);
        int seq = sequenceIds.get(taskId) + 1;
        List<Column> column = new ArrayList<>();
        Column col0 = new Column();
        col0.setDataType(DataType.INT.getType());
        col0.setColumnName("id");
        column.add(col0);
        Block block = new Block(column, 50 * 1024 * 1024, seq, taskId);
        block.setSequenceHasNext(false);
        sequenceIds.put(taskId, seq);

        return block;
    }

    private Block executeSelect(Task task)
    {
        String taskId = task.getTaskId();
        logger.info("Executing QueryTask " + taskId);

        if (!resultSetMap.containsKey(taskId)) {
            PardResultSet pardResultSet = connector.execute(task);
            resultSetMap.put(taskId, pardResultSet);
            sequenceIds.put(taskId, 0);
            schemaMap.put(taskId, pardResultSet.getSchema());
        }

        PardResultSet resultSet = resultSetMap.get(taskId);
        int seq = sequenceIds.get(taskId) + 1;
        Block block = new Block(schemaMap.get(taskId), 50 * 1024 * 1024, seq, taskId);
        block.setSequenceHasNext(false);
        sequenceIds.put(taskId, seq);
        Row row;
        while ((row = resultSet.getNext()) != null) {
            if (!block.addRow(row)) {
                block.setSequenceHasNext(true);
                break;
            }
        }
        logger.info("Result block num: " + block.getRows().size());
        if (!block.isSequenceHasNext()) {
            resultSetMap.remove(taskId);
            schemaMap.remove(taskId);
            sequenceIds.remove(taskId);
        }
        return block;
    }

    private int fileLength(File file)
    {
        int counter = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String str = null;
            while ((str = br.readLine()) != null) {
                counter++;
            }
            br.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return counter;
    }
}
