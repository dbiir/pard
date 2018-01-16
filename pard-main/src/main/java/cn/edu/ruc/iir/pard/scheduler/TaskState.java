package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Task;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TaskState
{
    private Map<String, Task> taskMap = null;
    private BlockingQueue<Block> blocks = null;
    private PardResultSet resultSet = null;
    public TaskState(Map<String, Task> taskMap, BlockingQueue<Block> blocks)
    {
        super();
        this.taskMap = taskMap;
        this.blocks = blocks;
    }
    public Map<String, Task> getTaskMap()
    {
        return taskMap;
    }
    public void setTaskMap(Map<String, Task> taskMap)
    {
        this.taskMap = taskMap;
    }
    public BlockingQueue<Block> getBlocks()
    {
        return blocks;
    }
    public int available()
    {
        return blocks.size();
    }
    public Block fetch()
    {
        Block block = null;
        do {
            try {
                block = blocks.poll(8000, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }while (block == null && !isDone());
        if (!block.isSequenceHasNext()) {
            String taskId = block.getTaskId();
            taskMap.remove(taskId);
        }
        return block;
    }
    public void setBlocks(BlockingQueue<Block> blocks)
    {
        this.blocks = blocks;
    }
    public PardResultSet getResultSet()
    {
        return resultSet;
    }
    public void setResultSet(PardResultSet resultSet)
    {
        this.resultSet = resultSet;
    }
    public boolean isDone()
    {
        return taskMap.isEmpty();
    }
}
