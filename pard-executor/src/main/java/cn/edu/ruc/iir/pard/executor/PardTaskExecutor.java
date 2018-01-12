package cn.edu.ruc.iir.pard.executor;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        String taskId = task.getTaskId();
        logger.info("Executing task " + taskId);

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
}
