package cn.edu.ruc.iir.pard.utils;

import cn.edu.ruc.iir.pard.memory.Block;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class ResultSet
{
    public enum ResultStatus
    {
        OK, PARSING_ERR, EXEC_ERR
    }

    private List<Block> blocks;
    private final ResultStatus resultStatus;

    public ResultSet(ResultStatus resultStatus)
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
