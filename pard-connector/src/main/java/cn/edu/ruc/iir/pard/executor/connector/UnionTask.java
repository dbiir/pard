package cn.edu.ruc.iir.pard.executor.connector;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class UnionTask
        extends Task
{
    /**
     *
     */
    private static final long serialVersionUID = -7442789904751335533L;
    private List<Task> waitTask;
    public UnionTask(String site)
    {
        super(site);
        waitTask = new ArrayList<Task>();
    }
    public List<Task> getWaitTask()
    {
        return waitTask;
    }
    public void setWaitTask(List<Task> waitTask)
    {
        this.waitTask = ImmutableList.copyOf(waitTask);
    }
}
