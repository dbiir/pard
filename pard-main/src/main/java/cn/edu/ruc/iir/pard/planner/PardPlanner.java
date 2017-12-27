package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.planner.ddl.SchemaCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.SchemaDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableCreationPlan;
import cn.edu.ruc.iir.pard.planner.ddl.TableDropPlan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.planner.dml.QueryPlan;
import cn.edu.ruc.iir.pard.sql.tree.CreateSchema;
import cn.edu.ruc.iir.pard.sql.tree.CreateTable;
import cn.edu.ruc.iir.pard.sql.tree.DropSchema;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.Query;
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
            return new QueryPlan(statement);
        }
        if (statement instanceof CreateSchema) {
            // create schema
            return new SchemaCreationPlan(statement);
        }
        if (statement instanceof CreateTable) {
            // create table
            return new TableCreationPlan(statement);
        }
        if (statement instanceof Use) {
            return new UsePlan(statement);
        }
        if (statement instanceof DropSchema) {
            return new SchemaDropPlan(statement);
        }
        if (statement instanceof DropTable) {
            return new TableDropPlan(statement);
        }

        return null;
    }
}
