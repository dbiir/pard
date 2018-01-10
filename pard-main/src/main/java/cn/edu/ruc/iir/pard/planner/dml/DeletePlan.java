package cn.edu.ruc.iir.pard.planner.dml;

import java.util.HashMap;
import java.util.Map;

import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Delete;
import cn.edu.ruc.iir.pard.sql.tree.DereferenceExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.IsNotNullPredicate;
import cn.edu.ruc.iir.pard.sql.tree.IsNullPredicate;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * pard
 *
 * @author guodong
 */
public class DeletePlan
        extends Plan
{
	 private Map<String, Expression> distributionHints;
    public DeletePlan(Statement stmt)
    {
        super(stmt);
    }
    
    public Map<String, Expression> getDistributionHints()
    {
        return this.distributionHints;
    }
    
    @Override
    public ErrorMessage semanticAnalysis()
    {
    	Statement statement = this.getStatment();
    	distributionHints = new HashMap<>();
    	if (!(statement instanceof Delete)) {
    	     return ErrorMessage.throwMessage(ErrCode.ParseError, "delete statement.");
    	}
    	String tableName = null;
    	String schemaName = null;
    	Schema schema = null;
    	Table table = null;
   
    	Delete delete = (Delete) statement;
    	
    	//check the table name
        tableName = delete.getName().getSuffix();
        if (delete.getName().getPrefix().isPresent()) {
            schemaName = delete.getName().getPrefix().get().toString();
        }
        boolean checkSchema = false;
        if (schemaName == null) {
            schema = UsePlan.getCurrentSchema();
            if (schema != null) {
                schemaName = schema.getName();
                checkSchema = true;
            }
        }
        if (schemaName == null) {
            return ErrorMessage.throwMessage(ErrCode.SchemaNotSpecified);
        }
        if (!checkSchema) {
            SchemaDao schemaDao = new SchemaDao();
            schema = schemaDao.loadByName(schemaName);
            if (schema == null) {
                return ErrorMessage.throwMessage(ErrCode.SchemaNotExsits, schemaName);
            }
        }
        TableDao tableDao = new TableDao(schema);
        table = tableDao.loadByName(tableName);
        if (table == null) {
            return ErrorMessage.throwMessage(ErrCode.TableNotExists, schemaName + "." + tableName);
        }
        Expression expression = delete.getExpression();
        ErrorMessage checkExpressionResult = checkExpression(table, expression);
        if(checkExpressionResult != null)
        {
        	return checkExpressionResult;
        }
        for (String key : table.getFragment().keySet()) {
            Fragment f = table.getFragment().get(key);
            distributionHints.put(f.getSiteName(), expression);
        }
        
        return null;
    }
    
    private ErrorMessage checkExpression(Table table, Expression expression)
    {
    	ErrorMessage result = null;
    	if(expression instanceof ComparisonExpression)
    	{
    		result = checkExpression(table, ((ComparisonExpression)expression).getLeft());
    		if(result == null)
    		{
    			result = checkExpression(table, ((ComparisonExpression)expression).getRight());

    		}   		    	
    	}else if(expression instanceof IsNullPredicate)
    	{
    		result =  checkExpression(table, ((IsNullPredicate)expression).getValue());
    	}else if(expression instanceof IsNotNullPredicate)
    	{
    		result = checkExpression(table, ((IsNotNullPredicate)expression).getValue());
    	}else if(expression instanceof IsNotNullPredicate)
    	{
    		result =  checkExpression(table, ((IsNotNullPredicate)expression).getValue());
    	}else if(expression instanceof Identifier)
    	{
    		if(!table.getColumns().keySet().contains(((Identifier)expression).getValue()))
    		{
    			return ErrorMessage.throwMessage(ErrorMessage.ErrCode.ColumnInTableNotFound);
    		}
    	}else if(expression instanceof DereferenceExpression)
    	{
    		result = checkExpression(table, ((DereferenceExpression)expression).getField());
    	}else
    	{
    		return ErrorMessage.throwMessage(ErrorMessage.ErrCode.UnSupportedQuery);
    	}
		return result;
    }
}
