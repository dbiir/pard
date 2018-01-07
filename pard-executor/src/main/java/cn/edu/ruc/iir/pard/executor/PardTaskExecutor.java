package cn.edu.ruc.iir.pard.executor;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Block;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * pard
 *
 * @author guodong
 */
public class PardTaskExecutor
{
    private final Map<String, PardResultSet> resultSetMap;    // taskId -> result set
    private final Map<String, List<String>> namesMap;         // taskId -> names
    private final Map<String, List<Integer>> typesMap;        // taskId -> types
    private final Map<String, Integer> sequenceIds;           // taskId -> sequence id
    private Connector connector;

    private PardTaskExecutor()
    {
        this.resultSetMap = new HashMap<>();
        this.namesMap = new HashMap<>();
        this.typesMap = new HashMap<>();
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

    public Block execute(Task task)
    {
        String taskId = task.getTaskId();

        if (!resultSetMap.containsKey(taskId)) {
            PardResultSet pardResultSet = connector.execute(task);
            List<String> names = new ArrayList<>();
            List<Integer> types = new ArrayList<>();
            resultSetMap.put(taskId, pardResultSet);
            for (Column column : pardResultSet.getSchema()) {
                names.add(column.getColumnName());
                types.add(column.getDataType());
            }
            namesMap.put(taskId, names);
            typesMap.put(taskId, types);
            sequenceIds.put(taskId, 0);
        }

        PardResultSet resultSet = resultSetMap.get(taskId);
        int seq = sequenceIds.get(taskId) + 1;
        Block block = new Block(namesMap.get(taskId), typesMap.get(taskId), 10 * 1024 * 1024, seq);
        sequenceIds.put(taskId, seq);
        Row row;
        while ((row = resultSet.getNext()) != null) {
            if (!block.addRow(row)) {
                block.setSequenceHasNext(true);
                break;
            }
        }
        if (block.isSequenceHasNext()) {
            resultSetMap.remove(taskId);
            namesMap.remove(taskId);
            typesMap.remove(taskId);
            sequenceIds.remove(taskId);
        }
        return block;
    }
}
