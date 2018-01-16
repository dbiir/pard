package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.commons.exception.SemanticException;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaShowPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableShowPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.DeletePlan;
import cn.edu.ruc.iir.pard.planner.dml.InsertPlan;
import cn.edu.ruc.iir.pard.planner.dml.LoadPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan2;
import cn.edu.ruc.iir.pard.sql.tree.CreateSchema;
import cn.edu.ruc.iir.pard.sql.tree.CreateTable;
import cn.edu.ruc.iir.pard.sql.tree.Delete;
import cn.edu.ruc.iir.pard.sql.tree.DropSchema;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.Insert;
import cn.edu.ruc.iir.pard.sql.tree.Load;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.ShowSchemas;
import cn.edu.ruc.iir.pard.sql.tree.ShowTables;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.sql.tree.Use;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * pard
 *
 * @author guodong
 */
public class PardPlanner
{
    public static Map<Class, Class> ast2plan = new HashMap<Class, Class>();
    public static void init()
    {
        ast2plan.put(Query.class, QueryPlan2.class);
        ast2plan.put(CreateSchema.class, SchemaCreationPlan.class);
        ast2plan.put(CreateTable.class, TableCreationPlan.class);
        ast2plan.put(Use.class, UsePlan.class);
        ast2plan.put(DropSchema.class, SchemaDropPlan.class);
        ast2plan.put(DropTable.class, TableDropPlan.class);
        ast2plan.put(Insert.class, InsertPlan.class);
        ast2plan.put(ShowSchemas.class, SchemaShowPlan.class);
        ast2plan.put(ShowTables.class, TableShowPlan.class);
        ast2plan.put(Load.class, LoadPlan.class);
        ast2plan.put(Delete.class, DeletePlan.class);
    }
    public Plan plan(Statement statement)
    {
        if (ast2plan.isEmpty()) {
            init();
        }
        Class planCls = ast2plan.get(statement.getClass());
        if (planCls == null) {
            return null;
        }
        try {
            Constructor con = planCls.getConstructor(new Class[]{Statement.class});
            return (Plan) con.newInstance(new Object[]{statement});
        }
        catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException e) {
            e.printStackTrace();
        }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof SemanticException) {
                throw (SemanticException) e.getCause();
            }
            else {
                e.printStackTrace();
            }
        }
        if (statement instanceof Query) {
            // query plan
            QueryPlan plan = new QueryPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof CreateSchema) {
            // create schema
            SchemaCreationPlan plan = new SchemaCreationPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof CreateTable) {
            // create table
            TableCreationPlan plan = new TableCreationPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof Use) {
            UsePlan plan = new UsePlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof DropSchema) {
            SchemaDropPlan plan = new SchemaDropPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof DropTable) {
            TableDropPlan plan = new TableDropPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof Insert) {
            InsertPlan plan = new InsertPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof ShowSchemas) {
            SchemaShowPlan plan = new SchemaShowPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof ShowTables) {
            TableShowPlan plan = new TableShowPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        if (statement instanceof Load) {
            LoadPlan plan = new LoadPlan(statement);
            return plan;
            /*
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }*/
        }
        return null;
    }
}
