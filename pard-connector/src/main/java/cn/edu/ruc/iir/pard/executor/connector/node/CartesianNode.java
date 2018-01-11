package cn.edu.ruc.iir.pard.executor.connector.node;

import java.util.ArrayList;
import java.util.List;

public class CartesianNode
        extends PlanNode
{
    private static final long serialVersionUID = -1634291095482760015L;
    protected List<PlanNode> childrens = new ArrayList<PlanNode>();
}
