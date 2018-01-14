package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.catalog.Column;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeHelper
{
    private NodeHelper()
    {
    }
    public static PlanNode copyNode(PlanNode node)
    {
        if (node == null) {
            return null;
        }
        if (node instanceof TableScanNode) {
            return new TableScanNode((TableScanNode) node);
        }
        else if (node instanceof UnionNode) {
            return new UnionNode((UnionNode) node);
        }
        else if (node instanceof SortNode) {
            return new SortNode((SortNode) node);
        }
        else if (node instanceof ProjectNode) {
            return new ProjectNode((ProjectNode) node);
        }
        else if (node instanceof OutputNode) {
            return new OutputNode((OutputNode) node);
        }
        else if (node instanceof LimitNode) {
            return new LimitNode((LimitNode) node);
        }
        else if (node instanceof JoinNode) {
            return new JoinNode((JoinNode) node);
        }
        else if (node instanceof FilterNode) {
            return new FilterNode((FilterNode) node);
        }
        else if (node instanceof DistinctNode) {
            return new DistinctNode((DistinctNode) node);
        }
        else if (node instanceof AggregationNode) {
            return new AggregationNode((AggregationNode) node);
        }
        return null;
    }
    public static String parseColumns(List<Column> list)
    {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i).getColumnName());
            if (i != list.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }
    public static String parseLists(List<Integer> list)
    {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i));
            if (i != list.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }
    public static List<PlanNode> getChildren(PlanNode node)
    {
        List<PlanNode> list = new ArrayList<PlanNode>();
        if (node instanceof UnionNode) {
            UnionNode union = (UnionNode) node;
            return union.getUnionChildren();
        }
        else if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            return join.getJoinChildren();
        }
        else if (node instanceof InputNode) {
            return list;
        }
        else {
            if (node.getLeftChild() != null) {
                list.add(node.getLeftChild());
            }
            return list;
        }
    }
    public static Map<String, String> getPlanNodeInfo(PlanNode node)
    {
        Map<String, String> obj = new HashMap<String, String>();
        if (node == null) {
            return obj;
        }
        if (node instanceof TableScanNode) {
            TableScanNode cnode = (TableScanNode) node;
            obj.put("name", "TABLESCAN");
            obj.put("schema", cnode.getSchema());
            obj.put("table", cnode.getTable());
            obj.put("site", cnode.getSite());
            //obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof UnionNode) {
            UnionNode cnode = (UnionNode) node;
            obj.put("name", "UNION");
            return obj;
        }
        else if (node instanceof SortNode) {
            SortNode cnode = (SortNode) node;
            obj.put("name", "SORT");
            obj.put("columns", parseColumns(cnode.getColumns()));
            obj.put("orderings", parseLists(cnode.getOrderings()));
            return obj;
        }
        else if (node instanceof ProjectNode) {
            ProjectNode cnode = (ProjectNode) node;
            obj.put("name", "PROJECT");
            obj.put("columns", parseColumns(cnode.getColumns()));
            return obj;
        }
        else if (node instanceof OutputNode) {
            OutputNode cnode = (OutputNode) node;
            obj.put("name", "OUTPUT");
            return obj;
        }
        else if (node instanceof LimitNode) {
            LimitNode cnode = (LimitNode) node;
            obj.put("name", "LIMIT");
            obj.put("number", ((LimitNode) node).getLimitNum() + "");
            return obj;
        }
        else if (node instanceof JoinNode) {
            JoinNode cnode = (JoinNode) node;
            obj.put("name", "JOIN");
            if (!cnode.getJoinSet().isEmpty()) {
                obj.put("joinColumn", cnode.getJoinSet().toString());
            }
            if (!cnode.getExprList().isEmpty()) {
                obj.put("exprList", cnode.getExprList().toString());
            }
            if (cnode.getOtherInfo() != null) {
                obj.put("mark", cnode.getOtherInfo());
            }
           // obj.put("joinSet", cnode.getJoinSet());
            return obj;
        }
        else if (node instanceof InputNode) {
            InputNode cnode = (InputNode) node;
            obj.put("name", "INPUT");
        }
        else if (node instanceof FilterNode) {
            FilterNode cnode = (FilterNode) node;
            obj.put("name", "FILTER");
            obj.put("expression", cnode.getExpression().toString());
            return obj;
        }
        else if (node instanceof DistinctNode) {
            DistinctNode cnode = (DistinctNode) node;
            obj.put("name", "DISTINCT");
            obj.put("columns", parseColumns(cnode.getColumns()));
            return obj;
        }
        else if (node instanceof AggregationNode) {
            AggregationNode cnode = (AggregationNode) node;
            obj.put("name", "AggragationNode");
            return obj;
        }
        else {
            System.out.println(node.getClass().getName());
        }
        return obj;
    }
    public static JSONObject toJSON(PlanNode node)
    {
        if (node instanceof TableScanNode) {
            TableScanNode cnode = (TableScanNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "TABLESCAN");
            obj.put("schema", cnode.getSchema());
            obj.put("table", cnode.getTable());
            obj.put("site", cnode.getSite());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof UnionNode) {
            UnionNode cnode = (UnionNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "UNION");
            JSONArray array = new JSONArray();
            for (PlanNode p : cnode.getUnionChildren()) {
                array.add(toJSON(p));
            }
            obj.put("children", array);
            return obj;
        }
        else if (node instanceof SortNode) {
            SortNode cnode = (SortNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "SORT");
            obj.put("columns", cnode.getColumns());
            obj.put("orderings", cnode.getOrderings());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof ProjectNode) {
            ProjectNode cnode = (ProjectNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "PROJECT");
            obj.put("columns", cnode.getColumns());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof OutputNode) {
            OutputNode cnode = (OutputNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "OUTPUT");
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof LimitNode) {
            LimitNode cnode = (LimitNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "LIMIT");
            obj.put("number", ((LimitNode) node).getLimitNum());
            obj.put("child", toJSON(cnode.getLeftChild()));
        }
        else if (node instanceof JoinNode) {
            JoinNode cnode = (JoinNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "JOIN");
            JSONArray array = new JSONArray();
            for (PlanNode p : cnode.getJoinChildren()) {
                array.add(toJSON(p));
            }
            obj.put("children", array);
            obj.put("joinSet", cnode.getJoinSet());
        }
        else if (node instanceof InputNode) {
            InputNode cnode = (InputNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "INPUT");
        }
        else if (node instanceof FilterNode) {
            FilterNode cnode = (FilterNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "FILTER");
            obj.put("expression", cnode.getExpression().toString());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof DistinctNode) {
            DistinctNode cnode = (DistinctNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "DISTINCT");
            obj.put("columns", cnode.getColumns());
            obj.put("child", toJSON(cnode.getLeftChild()));
            return obj;
        }
        else if (node instanceof AggregationNode) {
            AggregationNode cnode = (AggregationNode) node;
            JSONObject obj = new JSONObject();
            obj.put("name", "AggragationNode");
            return obj;
        }
        return null;
    }
}
