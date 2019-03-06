using Antlr4.Runtime.Misc;
using PrismaDB.Commons;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MySQL.AntlrGrammer;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.MySQL
{
    public partial class MySqlVisitor : MySqlParserBaseVisitor<object>
    {
        public override object VisitInsertStatement([NotNull] MySqlParser.InsertStatementContext context)
        {
            var res = new InsertQuery();
            res.Into = (TableRef)Visit(context.tableName());
            if (context.uidList() != null)
                foreach (var id in (List<Identifier>)Visit(context.uidList()))
                    res.Columns.Add(new ColumnRef(id));
            res.Values = (List<List<Expression>>)Visit(context.insertStatementValue());
            return res;
        }

        public override object VisitSelectStatement([NotNull] MySqlParser.SelectStatementContext context)
        {
            var res = new SelectQuery();
            res.SelectExpressions = (List<Expression>)Visit(context.selectElements());
            if (context.fromClause() != null)
            {
                var from = (SelectQuery)Visit(context.fromClause());
                res.FromTables = from.FromTables;
                res.Joins = from.Joins;
            }
            if (context.whereClause() != null)
                res.Where = (WhereClause)Visit(context.whereClause());
            if (context.groupByClause() != null)
                res.GroupBy = (GroupByClause)Visit(context.groupByClause());
            if (context.orderByClause() != null)
                res.OrderBy = (OrderByClause)Visit(context.orderByClause());
            if (context.limitClause() != null)
                res.Limit = (uint?)Visit(context.limitClause());
            return res;
        }

        public override object VisitInsertStatementValue([NotNull] MySqlParser.InsertStatementValueContext context)
        {
            var res = new List<List<Expression>>();
            foreach (var exps in context.expressions())
                res.Add((List<Expression>)Visit(exps));
            return res;
        }

        public override object VisitUpdatedElement([NotNull] MySqlParser.UpdatedElementContext context)
        {
            var res = new Pair<ColumnRef, Constant>();
            res.First = (ColumnRef)Visit(context.fullColumnName());
            res.Second = (Constant)Visit(context.expression());
            return res;
        }

        public override object VisitSingleDeleteStatement([NotNull] MySqlParser.SingleDeleteStatementContext context)
        {
            var res = new DeleteQuery();
            res.DeleteTable = (TableRef)Visit(context.tableName());
            if (context.expression() != null)
                res.Where = ExpressionToCnfWhere(context.expression());
            return res;
        }

        public override object VisitSingleUpdateStatement([NotNull] MySqlParser.SingleUpdateStatementContext context)
        {
            var res = new UpdateQuery();
            res.UpdateTable = (TableRef)Visit(context.tableName());
            foreach (var updatedElement in context.updatedElement())
                res.UpdateExpressions.Add((Pair<ColumnRef, Constant>)Visit(updatedElement));
            if (context.expression() != null)
                res.Where = ExpressionToCnfWhere(context.expression());
            return res;
        }

        public override object VisitOrderByClause([NotNull] MySqlParser.OrderByClauseContext context)
        {
            var res = new OrderByClause();
            foreach (var orderByExp in context.orderByExpression())
                res.OrderColumns.Add((Pair<ColumnRef, OrderDirection>)Visit(orderByExp));
            return res;
        }

        public override object VisitOrderByExpression([NotNull] MySqlParser.OrderByExpressionContext context)
        {
            var res = new Pair<ColumnRef, OrderDirection>();
            res.First = (ColumnRef)Visit(context.expression());
            res.Second = OrderDirection.ASC;
            if (context.DESC() != null)
                res.Second = OrderDirection.DESC;
            return res;
        }

        public override object VisitTableSources([NotNull] MySqlParser.TableSourcesContext context)
        {
            var res = new List<TableRef>();
            foreach (var tableSource in context.tableSourceItem())
                res.Add((TableRef)Visit(tableSource));
            return res;
        }

        public override object VisitTableSourceItem([NotNull] MySqlParser.TableSourceItemContext context)
        {
            var res = (TableRef)Visit(context.tableName());
            if (context.alias != null)
                res.Alias = (Identifier)Visit(context.uid());
            return res;
        }

        public override object VisitInnerJoin([NotNull] MySqlParser.InnerJoinContext context)
        {
            var res = new JoinClause();
            res.JoinType = JoinType.INNER;
            if (context.CROSS() != null)
                res.JoinType = JoinType.CROSS;
            res.JoinTable = (TableRef)Visit(context.tableSourceItem());
            if (context.ON() != null)
            {
                var exp = (BooleanEquals)Visit(context.expression());
                res.FirstColumn = (ColumnRef)exp.left;
                res.SecondColumn = (ColumnRef)exp.right;
            }
            return res;
        }

        public override object VisitOuterJoin([NotNull] MySqlParser.OuterJoinContext context)
        {
            var res = new JoinClause();
            if (context.LEFT() != null)
                res.JoinType = JoinType.LEFT_OUTER;
            else if (context.RIGHT() != null)
                res.JoinType = JoinType.RIGHT_OUTER;
            res.JoinTable = (TableRef)Visit(context.tableSourceItem());
            if (context.ON() != null)
            {
                var exp = (BooleanEquals)Visit(context.expression());
                res.FirstColumn = (ColumnRef)exp.left;
                res.SecondColumn = (ColumnRef)exp.right;
            }
            return res;
        }

        public override object VisitSelectElements([NotNull] MySqlParser.SelectElementsContext context)
        {
            var res = new List<Expression>();

            if (context.star != null)
                res.Add(new AllColumns());

            foreach (var element in context.selectElement())
                res.Add((Expression)Visit(element));

            return res;
        }

        public override object VisitSelectStarElement([NotNull] MySqlParser.SelectStarElementContext context)
        {
            return new AllColumns(((Identifier)Visit(context.uid())).id);
        }

        public override object VisitSelectColumnElement([NotNull] MySqlParser.SelectColumnElementContext context)
        {
            var res = (ColumnRef)Visit(context.fullColumnName());
            if (context.uid() != null)
                res.Alias = (Identifier)Visit(context.uid());
            return res;
        }

        public override object VisitSelectFunctionElement([NotNull] MySqlParser.SelectFunctionElementContext context)
        {
            var res = (ScalarFunction)Visit(context.functionCall());
            if (context.uid() != null)
                res.Alias = (Identifier)Visit(context.uid());
            else
                res.Alias = new Identifier(context.functionCall().GetText());
            return res;
        }

        public override object VisitSelectExpressionElement([NotNull] MySqlParser.SelectExpressionElementContext context)
        {
            var res = (Expression)Visit(context.expression());
            if (context.uid() != null)
                res.Alias = (Identifier)Visit(context.uid());
            else
                res.Alias = new Identifier(context.expression().GetText());
            return res;
        }

        public override object VisitFromClause([NotNull] MySqlParser.FromClauseContext context)
        {
            var res = new SelectQuery();
            res.FromTables = (List<TableRef>)Visit(context.tableSources());
            res.Joins = new List<JoinClause>();
            foreach (var joinPart in context.joinPart())
                res.Joins.Add((JoinClause)Visit(joinPart));
            return res;
        }

        public override object VisitWhereClause([NotNull] MySqlParser.WhereClauseContext context)
        {
            return ExpressionToCnfWhere(context.whereExpr);
        }

        public override object VisitGroupByClause([NotNull] MySqlParser.GroupByClauseContext context)
        {
            var res = new GroupByClause();
            foreach (var groupByItem in context.groupByItem())
                res.GroupColumns.Add((ColumnRef)Visit(groupByItem));
            return res;
        }

        public override object VisitGroupByItem([NotNull] MySqlParser.GroupByItemContext context)
        {
            return Visit(context.expression());
        }

        public override object VisitLimitClause([NotNull] MySqlParser.LimitClauseContext context)
        {
            return (uint?)((IntConstant)Visit(context.intLiteral())).intvalue;
        }

        public WhereClause ExpressionToCnfWhere(MySqlParser.ExpressionContext context)
        {
            var res = new WhereClause();
            var expr = (Expression)Visit(context);
            while (!CnfConverter.CheckCnf(expr)) expr = CnfConverter.ConvertToCnf(expr);
            res.CNF = CnfConverter.BuildCnf(expr);
            return res;
        }
    }
}
