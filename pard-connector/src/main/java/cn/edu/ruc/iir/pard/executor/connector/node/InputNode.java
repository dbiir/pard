package cn.edu.ruc.iir.pard.executor.connector.node;

/**
 * pard
 *
 * @author guodong
 */
public abstract class InputNode
        extends PlanNode
{
    private static final long serialVersionUID = 7191268988916378730L;
    public InputNode()
    {
        this.name = "INPUT";
    }

    public InputNode(InputNode node)
    {
        super(node);
        this.name = "INPUT";
    }
}
