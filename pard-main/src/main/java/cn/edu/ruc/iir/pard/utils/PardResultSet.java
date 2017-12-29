package cn.edu.ruc.iir.pard.utils;

import cn.edu.ruc.iir.pard.memory.Block;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class PardResultSet
{
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
