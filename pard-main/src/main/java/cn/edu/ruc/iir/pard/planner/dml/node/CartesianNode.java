package cn.edu.ruc.iir.pard.planner.dml.node;

import cn.edu.ruc.iir.pard.planner.PlanNode;

import java.util.ArrayList;
import java.util.List;

public class CartesianNode
        extends PlanNode
{
    private List<PlanNode> children = new ArrayList<PlanNode>();
}
