package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.planner.ddl.SchemaCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaShowPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableShowPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.InsertPlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.sql.tree.CreateSchema;
import cn.edu.ruc.iir.pard.sql.tree.CreateTable;
import cn.edu.ruc.iir.pard.sql.tree.DropSchema;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.Insert;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.ShowSchemas;
import cn.edu.ruc.iir.pard.sql.tree.ShowTables;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.sql.tree.Use;

/**
 * pard
 *
 * @author guodong
 */
public class PardPlanner
{
    public Plan plan(Statement statement)
    {
        if (statement instanceof Query) {
            // query plan
            QueryPlan plan = new QueryPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof CreateSchema) {
            // create schema
            SchemaCreationPlan plan = new SchemaCreationPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof CreateTable) {
            // create table
            TableCreationPlan plan = new TableCreationPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof Use) {
            UsePlan plan = new UsePlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof DropSchema) {
            SchemaDropPlan plan = new SchemaDropPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof DropTable) {
            TableDropPlan plan = new TableDropPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof Insert) {
            InsertPlan plan = new InsertPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof ShowSchemas) {
            SchemaShowPlan plan = new SchemaShowPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        if (statement instanceof ShowTables) {
            TableShowPlan plan = new TableShowPlan(statement);
            ErrorMessage errorMessage = plan.semanticAnalysis();
            if (errorMessage.getErrcode() == ErrorMessage.ErrCode.OK) {
                return plan;
            }
        }
        return null;
    }
}
