package cn.edu.ruc.iir.pard.executor.connector.node;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JSONHelper
{
    private JSONHelper()
    {
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
            for (PlanNode p : cnode.getUnionChildren()) {
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
