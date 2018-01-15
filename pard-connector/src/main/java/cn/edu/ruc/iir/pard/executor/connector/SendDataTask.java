package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.sql.tree.Expression;

import java.util.HashMap;
import java.util.Map;

public class SendDataTask
        extends Task
{
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String taskId;
    private String schemaName;
    private PlanNode node;
    private Map<String, Expression> siteExpression; // site -> Expression
    private Map<String, String> tmpTableMap; // site -> tmpTableName
    public SendDataTask(String site)
    {
        super(site);
        siteExpression = new HashMap<String, Expression>();
        tmpTableMap = new HashMap<String, String>();
    }

    public String getTaskId()
    {
        return taskId;
    }

    public void setTaskId(String taskId)
    {
        this.taskId = taskId;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public PlanNode getNode()
    {
        return node;
    }

    public void setNode(PlanNode node)
    {
        this.node = node;
    }

    public Map<String, Expression> getSiteExpression()
    {
        return siteExpression;
    }

    public void setSiteExpression(Map<String, Expression> siteExpression)
    {
        this.siteExpression = siteExpression;
    }

    public Map<String, String> getTmpTableMap()
    {
        return tmpTableMap;
    }

    public void setTmpTableMap(Map<String, String> tmpTableMap)
    {
        this.tmpTableMap = tmpTableMap;
    }
}
