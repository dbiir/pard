package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.planner.PardPlanner;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.scheduler.JobScheduler.JobState;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonValueProcessor;
import org.testng.annotations.Test;

import java.util.List;

public class TaskSchedulerTest
{
    SqlParser parser = new SqlParser();
    @Test
    public void test2()
    {
        UsePlan.setCurrentSchema("book");
        String sql = "select Book.title,Book.copies,Publisher.name,Publisher.nation from Book,Publisher where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies > 1000";
        Statement stmt = parser.createStatement(sql);
        PardPlanner planner = new PardPlanner();
        Plan plan = planner.plan(stmt);
        plan.setJobId("aa");
        List<Task> task = TaskScheduler.INSTANCE().generateTasks(plan);
        JsonConfig config = new JsonConfig();
        config.setExcludes(new String[]{"rightChild"});
        config.registerJsonValueProcessor(Expression.class, new JsonValueProcessor(){
            @Override
            public Object processArrayValue(Object arg0, JsonConfig arg1)
            {
                return arg0.toString();
            }

            @Override
            public Object processObjectValue(String arg0, Object arg1, JsonConfig arg2)
            {
                return arg1.toString();
            }
        });
        config.registerJsonValueProcessor(ComparisonExpression.class, new JsonValueProcessor(){
            @Override
            public Object processArrayValue(Object arg0, JsonConfig arg1)
            {
                return arg0.toString();
            }

            @Override
            public Object processObjectValue(String arg0, Object arg1, JsonConfig arg2)
            {
                return arg1.toString();
            }
        });
        config.registerJsonValueProcessor(LogicalBinaryExpression.class, new JsonValueProcessor(){
            @Override
            public Object processArrayValue(Object arg0, JsonConfig arg1)
            {
                return arg0.toString();
            }

            @Override
            public Object processObjectValue(String arg0, Object arg1, JsonConfig arg2)
            {
                return arg1.toString();
            }
        });
        config.registerJsonValueProcessor(Column.class, new JsonValueProcessor(){
            @Override
            public Object processArrayValue(Object arg0, JsonConfig arg1)
            {
                JSONObject obj = new JSONObject();
                Column col = (Column) arg0;
                obj.put("columnName", col.getColumnName());
                obj.put("tableName", col.getTableName());
                return obj;
            }

            @Override
            public Object processObjectValue(String arg0, Object arg1, JsonConfig arg2)
            {
                JSONObject obj = new JSONObject();
                Column col = (Column) arg1;
                obj.put("columnName", col.getColumnName());
                obj.put("tableName", col.getTableName());
                return obj;
            }
        });
        System.out.println(JSONArray.fromObject(task, config).toString(1));
        Job job = JobScheduler.INSTANCE().newJob();
        task.forEach(job::addTask);
        job.setJobState(JobState.EXECUTING);
        job.setPlan(plan);
       // TaskScheduler.INSTANCE().executeJob(job);
    }
}
