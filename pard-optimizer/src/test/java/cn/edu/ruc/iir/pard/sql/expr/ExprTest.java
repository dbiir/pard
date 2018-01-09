package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.rules.ContainEliminateLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.MinimalItemLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.PushDownLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.TrueFalseLaw;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.QueryBody;
import cn.edu.ruc.iir.pard.sql.tree.QuerySpecification;
import cn.edu.ruc.iir.pard.sql.tree.Select;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.Optional;

public class ExprTest
{
    //ComparisonExpression
    //DereferenceExpression
    //inListExpression
    //LogicalBinaryExpression
    //Not Expression
    //Subquery Expression
    @Test
    public void pipeline()
    {
        String sql = "select a,b,c from D,E where D.id=E.id and (d.a=1 or E.b=2 and C.k>3 or not (p.a<5 and p.d>7) and true) and d.c<10 and d.c<5";
        SqlParser parser = new SqlParser();
        Statement stmt = parser.createStatement(sql);
        Query query = (Query) stmt;
        QueryBody queryBody = query.getQueryBody();
        QuerySpecification querySpecification = (QuerySpecification) queryBody;
        Select select = querySpecification.getSelect();
        Optional<Expression> oexpr = querySpecification.getWhere();
        if (oexpr.isPresent()) {
            Expression expr = oexpr.get();
            Expr e = Expr.parse(expr);
            System.out.println(e.toString());
            if (e instanceof CompositionExpr) {
                PushDownLaw pl = new PushDownLaw(LogicOperator.AND);
                ContainEliminateLaw cl = new ContainEliminateLaw();
                TrueFalseLaw tfLaw = new TrueFalseLaw();
                MinimalItemLaw milaw = new MinimalItemLaw();
                CompositionExpr exps = (CompositionExpr) e;
                Expr pase = pl.apply(exps); //下推AND
                //pase = cl.apply(pase); //合并处理蕴含, 有问题
                //（a or (a and b) => (a and b)） 有问题
                // （a and (a or b) => a）
                Expr ex = milaw.apply(milaw.apply(pase)); //合并a>10 & a>15这样的项
                System.out.println(ex);
                System.out.println(tfLaw.apply(ex)); // 永真式永假式判断
                //System.out.println(pl.apply(pase));
            }
        }
    }
    @Test
    public void justTest()
    {
        String sql = "select a,b,c from D,E where D.id=E.id and (d.a=1 or E.b=2 and C.k>3 or not (p.a<5 and p.d>7) and true) and d.c<10 and d.c>10";
        //String sql = "select a,b,c from D,E where a.b=1 and (c.d=1 or d.e=1)";
        SqlParser parser = new SqlParser();
        Statement stmt = parser.createStatement(sql);
        //System.out.println(stmt.toString());
        Query query = (Query) stmt;
        QueryBody queryBody = query.getQueryBody();
        QuerySpecification querySpecification = (QuerySpecification) queryBody;
        Select select = querySpecification.getSelect();
        Optional<Expression> oexpr = querySpecification.getWhere();
        if (oexpr.isPresent()) {
            Expression expr = oexpr.get();
            System.out.println(expr.toString());
            System.out.println(expr.getClass().getName());
            if (expr instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression lbe = (LogicalBinaryExpression) expr;
                Expression left = lbe.getLeft();
                Expression right = lbe.getRight();
                System.out.println(left.getClass().getName());
                System.out.println(right.getClass().getName());
                if (left instanceof ComparisonExpression) {
                    ComparisonExpression ce = (ComparisonExpression) left;
                    System.out.println(ce.getLeft().getClass().getName());
                    System.out.println(ce.getRight().getClass().getName());
                }
            }
            Expr e = Expr.parse(expr);
            System.out.println(e.toString());
            if (e instanceof CompositionExpr) {
                PushDownLaw pl = new PushDownLaw(LogicOperator.AND);
                CompositionExpr exps = (CompositionExpr) e;
                Expr pase = pl.apply(exps);
                System.out.println(pase.toString());
                PushDownLaw pl2 = new PushDownLaw(LogicOperator.OR);
                ContainEliminateLaw cl = new ContainEliminateLaw();
                System.out.println(cl.apply(pl2.apply(pase)));
                System.out.println(cl.apply(pl2.apply(pl2.apply(exps))));
                TrueFalseLaw tfLaw = new TrueFalseLaw();
                System.out.println(tfLaw.apply(pase));
                MinimalItemLaw milaw = new MinimalItemLaw();
                Expr ex = milaw.apply(pase);
                System.out.println(ex);
                System.out.println(tfLaw.apply(ex));
                //System.out.println(pl.apply(pase));
            }
        }
    }
}
