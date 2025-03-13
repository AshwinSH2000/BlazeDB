//package ed.inf.adbs.blazedb;
//import net.sf.jsqlparser.expression.Expression;
//import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
//
//
//
//import java.util.List;
//
//public class ExpressionCombiner {
//
//    public static Expression combineWithAnd(List<Expression> expressions) {
//        if (expressions == null || expressions.isEmpty()) {
//            return null;  // No expressions to combine
//        }
//
//        // Start with the first expression
//        Expression combinedExpression = expressions.get(0);
//
//        // Iterate through the list and combine the expressions with AND
//        for (int i = 1; i < expressions.size(); i++) {
//            combinedExpression = new AndExpression(combinedExpression, expressions.get(i));
//        }
//
//        return combinedExpression;
//    }
//}