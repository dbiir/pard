package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class JoinTask
        extends Task
{
    /**
     *
     */
    private static final long serialVersionUID = 8086274031234684990L;
    private List<Task> waitTask;
    private String verticalJoinColumn;
    private List<Expression> joinList;
    public JoinTask(String site)
    {
        super(site);
        verticalJoinColumn = null;
        joinList = new ArrayList<Expression>();
    }
    public List<Task> getWaitTask()
    {
        return waitTask;
    }
    public void setWaitTask(List<Task> waitTask)
    {
        this.waitTask = ImmutableList.copyOf(waitTask);
    }
    public String getVerticalJoinColumn()
    {
        return verticalJoinColumn;
    }
    public void setVerticalJoinColumn(String verticalJoinColumn)
    {
        this.verticalJoinColumn = verticalJoinColumn;
    }
    public List<Expression> getJoinList()
    {
        return joinList;
    }
    public void setJoinList(List<Expression> joinList)
    {
        this.joinList = joinList;
    }
}
