package cn.edu.ruc.iir.pard.commons.utils;

import cn.edu.ruc.iir.pard.commons.memory.Block;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class PardResultSet
        implements Serializable
{
    private static final long serialVersionUID = 8184501795566412803L;

    public enum ResultStatus
    {
        OK(""),
        BEGIN_ERR("Create job error"),
        PARSING_ERR("Parse error"),
        PLANNING_ERR("Plan error"),
        SCHEDULING_ERR("Schedule error"),
        EXECUTING_ERR("Execution error");

        private String msg;

        ResultStatus(String msg)
        {
            this.msg = msg;
        }

        @Override
        public String toString()
        {
            return this.msg;
        }
    }

    private List<Block> blocks;
    private final ResultStatus resultStatus;

    public PardResultSet(ResultStatus resultStatus)
    {
        blocks = new ArrayList<>();
        this.resultStatus = resultStatus;
    }

    public void addBlock(Block block)
    {
        blocks.add(block);
    }

    public boolean hasNext()
    {
        return blocks.size() > 0;
    }

    public Block getNext()
    {
        return blocks.remove(blocks.size());
    }

    public ResultStatus getStatus()
    {
        return resultStatus;
    }
}
