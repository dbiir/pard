package cn.edu.ruc.iir.pard.web;

import cn.edu.ruc.iir.pard.executor.connector.node.InputNode;
import cn.edu.ruc.iir.pard.executor.connector.node.NodeHelper;
import cn.edu.ruc.iir.pard.executor.connector.node.OutputNode;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.planner.PardPlanner;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import net.sf.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class PardServlet
        extends HttpServlet
{
    private static final long serialVersionUID = 6154102774532361192L;
    //public static List<QueryPlan> planList =
    int keyGen = 0;
    public PNode parse(PlanNode pnode)
    {
        PNode pn = new PNode();
        pn.setKey(pnode.getName() + (++keyGen));
        StringBuilder sb = new StringBuilder();
        Map<String, String> map = NodeHelper.getPlanNodeInfo(pnode);
        for (String key : map.keySet()) {
            sb.append(key).append(":").append(map.get(key)).append("\n");
        }
        pn.setText(sb.toString());
        pn.setFigure("Rectangle");
        pn.setFill("#4fba4f");
        pn.setStepType(0);
        if (pnode instanceof OutputNode) {
            pn.setStepType(1);
        }
        else if (pnode instanceof InputNode) {
            pn.setStepType(4);
        }
        return pn;
    }
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        keyGen = 0;
        PardPlanner planner = new PardPlanner();
        Statement stmt = new SqlParser().createStatement("SELECT * FROM pardtest.emp where eno < 'E0010' and eno > 'E0000'");
        Plan plan = planner.plan(stmt);
        List<PNode> nodeDataArray = new ArrayList<PNode>();
        List<PEdge> linkedDataArray = new ArrayList<PEdge>();
        if (plan instanceof QueryPlan) {
            PlanNode node = ((QueryPlan) plan).optimize();
            Map<PlanNode, PNode> mapping = new HashMap<PlanNode, PNode>();
            Queue<PlanNode> que = new LinkedList<PlanNode>();
            que.add(node);
            PNode pa = parse(node);
            mapping.put(node, pa);
            nodeDataArray.add(pa);
            while (!que.isEmpty()) {
                PlanNode planNode = que.poll();
                PNode parent = mapping.get(planNode);
                List<PlanNode> pnlist = NodeHelper.getChildren(planNode);
                for (PlanNode pnode : pnlist) {
                    PNode p = parse(pnode);
                    mapping.put(pnode, p);
                    PEdge e = new PEdge();
                    e.from = parent.getKey();
                    e.to = p.getKey();
                    nodeDataArray.add(p);
                    linkedDataArray.add(e);
                    que.add(pnode);
                }
            }
            JSONObject json = new JSONObject();
            json.put("class", "go.GraphLinksModel");
            JSONObject pos = new JSONObject();
            pos.put("position", "-5 -5");
            json.put("modelData", pos);
            json.put("nodeDataArray", nodeDataArray);
            json.put("linkDataArray", linkedDataArray);
            System.out.println(json.toString());
            String body = readHtml("webapp/_demo.html");
            String f1 = "<textarea id=\"mySavedModel\" style=\"width:100%;height:300px\">";
            int pos1 = body.indexOf(f1) + f1.length();
            int pos2 = body.indexOf("</textarea>");
            String re = body.substring(pos1, pos2);
            body = body.replace(re, json.toString(1));
            resp.getWriter().write(body);
        }
    }
    public String readHtml(String str) throws IOException
    {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(str);
        StringBuilder sb = new StringBuilder();
        if (in != null) {
            int n = 0;
            byte[] b = new byte[4096];
            while ((n = in.read(b)) != -1) {
                //resp.getOutputStream().write(b, 0, n);
                sb.append(new String(b, 0, n, "UTF-8"));
            }
            in.close();
        }
        //System.out.println(sb.toString());
        return sb.toString();
    }
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        // TODO Auto-generated method stub
        super.doPost(req, resp);
    }

    public static class PEdge
    {
        private String from;
        private String to;
        public String getFrom()
        {
            return from;
        }
        public void setFrom(String from)
        {
            this.from = from;
        }
        public String getTo()
        {
            return to;
        }
        public void setTo(String to)
        {
            this.to = to;
        }
    }
}
