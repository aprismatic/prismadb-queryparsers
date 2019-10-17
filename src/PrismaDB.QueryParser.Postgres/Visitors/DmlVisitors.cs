using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.Postgres.AntlrGrammer;
using System;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.Postgres
{
    public partial class PostgresVisitor : PostgresParserBaseVisitor<object>
    {
        public override object VisitInsertStatement([NotNull] PostgresParser.InsertStatementContext context)
        {
            var res = new InsertQuery();
            res.Into = (TableRef)Visit(context.tableName());
            if (context.uidList() != null)
                foreach (var id in (List<Identifier>)Visit(context.uidList()))
                    res.Columns.Add(new ColumnRef(id));
            res.Values = (List<List<Expression>>)Visit(context.insertStatementValue());
            return res;
        }

        public override object VisitSelectStatement([NotNull] PostgresParser.SelectStatementContext context)
        {
            var res = new SelectQuery();
            res.SelectExpressions = (List<Expression>)Visit(context.selectElements());
            if (context.fromClause() != null)
                res.From = (FromClause)Visit(context.fromClause());
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

        public override object VisitInsertStatementValue([NotNull] PostgresParser.InsertStatementValueContext context)
        {
            var res = new List<List<Expression>>();
            foreach (var exps in context.expressions())
                res.Add((List<Expression>)Visit(exps));
            return res;
        }

        public override object VisitUpdatedElement([NotNull] PostgresParser.UpdatedElementContext context)
        {
            return new Tuple<ColumnRef, ConstantContainer>((ColumnRef)Visit(context.fullColumnName()), (ConstantContainer)Visit(context.expression()));
        }

        public override object VisitSingleDeleteStatement([NotNull] PostgresParser.SingleDeleteStatementContext context)
        {
            var res = new DeleteQuery();
            res.DeleteTable = (TableRef)Visit(context.tableName());
            if (context.expression() != null)
                res.Where = ExpressionToCnfWhere(context.expression());
            return res;
        }

        public override object VisitSingleUpdateStatement([NotNull] PostgresParser.SingleUpdateStatementContext context)
        {
            var res = new UpdateQuery();
            res.UpdateTable = (TableRef)Visit(context.tableName());
            foreach (var updatedElement in context.updatedElement())
                res.UpdateExpressions.Add((Tuple<ColumnRef, ConstantContainer>)Visit(updatedElement));
            if (context.expression() != null)
                res.Where = ExpressionToCnfWhere(context.expression());
            return res;
        }

        public override object VisitOrderByClause([NotNull] PostgresParser.OrderByClauseContext context)
        {
            var res = new OrderByClause();
            foreach (var orderByExp in context.orderByExpression())
                res.OrderColumns.Add((Tuple<ColumnRef, OrderDirection>)Visit(orderByExp));
            return res;
        }

        public override object VisitOrderByExpression([NotNull] PostgresParser.OrderByExpressionContext context)
        {
            if (context.DESC() != null)
                return new Tuple<ColumnRef, OrderDirection>((ColumnRef)Visit(context.expression()), OrderDirection.DESC);

            return new Tuple<ColumnRef, OrderDirection>((ColumnRef)Visit(context.expression()), OrderDirection.ASC);
        }

        public override object VisitTableSources([NotNull] PostgresParser.TableSourcesContext context)
        {
            var res = new List<FromSource>();
            foreach (var tableSource in context.tableSource())
                res.Add((FromSource)Visit(tableSource));
            return res;
        }

        public override object VisitTableSource([NotNull] PostgresParser.TableSourceContext context)
        {
            var res = new FromSource();
            res.FirstTable = (SingleTable)Visit(context.tableSourceItem());
            foreach (var joinedTable in context.joinPart())
                res.JoinedTables.Add((JoinedTable)Visit(joinedTable));
            return res;
        }

        public override object VisitAtomTableItem([NotNull] PostgresParser.AtomTableItemContext context)
        {
            var res = new TableSource();
            res.Table = (TableRef)Visit(context.tableName());
            if (context.alias != null)
                res.Table.Alias = (Identifier)Visit(context.alias);
            return res;
        }

        public override object VisitSubqueryTableItem([NotNull] PostgresParser.SubqueryTableItemContext context)
        {
            var res = new SelectSubQuery();
            res.Select = (SelectQuery)Visit(context.selectStatement());
            res.Alias = (Identifier)Visit(context.alias);
            return res;
        }

        public override object VisitInnerJoin([NotNull] PostgresParser.InnerJoinContext context)
        {
            var res = new JoinedTable();
            res.JoinType = JoinType.INNER;
            if (context.CROSS() != null)
                res.JoinType = JoinType.CROSS;
            res.SecondTable = (SingleTable)Visit(context.tableSourceItem());
            if (context.ON() != null)
            {
                var exp = (BooleanEquals)Visit(context.expression());
                res.FirstColumn = (ColumnRef)exp.left;
                res.SecondColumn = (ColumnRef)exp.right;
            }
            return res;
        }

        public override object VisitOuterJoin([NotNull] PostgresParser.OuterJoinContext context)
        {
            var res = new JoinedTable();
            if (context.LEFT() != null)
                res.JoinType = JoinType.LEFT_OUTER;
            else if (context.RIGHT() != null)
                res.JoinType = JoinType.RIGHT_OUTER;
            else if (context.FULL() != null)
                res.JoinType = JoinType.FULL_OUTER;
            res.SecondTable = (SingleTable)Visit(context.tableSourceItem());
            if (context.ON() != null)
            {
                var exp = (BooleanEquals)Visit(context.expression());
                res.FirstColumn = (ColumnRef)exp.left;
                res.SecondColumn = (ColumnRef)exp.right;
            }
            return res;
        }

        public override object VisitSelectElements([NotNull] PostgresParser.SelectElementsContext context)
        {
            var res = new List<Expression>();

            if (context.star != null)
                res.Add(new AllColumns());

            foreach (var element in context.selectElement())
                res.Add((Expression)Visit(element));

            return res;
        }

        public override object VisitSelectStarElement([NotNull] PostgresParser.SelectStarElementContext context)
        {
            return new AllColumns(((Identifier)Visit(context.uid())).id);
        }

        public override object VisitSelectColumnElement([NotNull] PostgresParser.SelectColumnElementContext context)
        {
            var res = (ColumnRef)Visit(context.fullColumnName());
            if (context.uid() != null)
                res.Alias = (Identifier)Visit(context.uid());
            return res;
        }

        public override object VisitSelectFunctionElement([NotNull] PostgresParser.SelectFunctionElementContext context)
        {
            var res = (ScalarFunction)Visit(context.functionCall());
            if (context.uid() != null)
                res.Alias = (Identifier)Visit(context.uid());
            else
                res.Alias = new Identifier(context.functionCall().GetText());
            return res;
        }

        public override object VisitSelectExpressionElement([NotNull] PostgresParser.SelectExpressionElementContext context)
        {
            var res = (Expression)Visit(context.expression());
            if (context.uid() != null)
                res.Alias = (Identifier)Visit(context.uid());
            else
                res.Alias = new Identifier(context.expression().GetText());
            return res;
        }

        public override object VisitFromClause([NotNull] PostgresParser.FromClauseContext context)
        {
            var res = new FromClause();
            res.Sources = (List<FromSource>)Visit(context.tableSources());
            return res;
        }

        public override object VisitWhereClause([NotNull] PostgresParser.WhereClauseContext context)
        {
            return ExpressionToCnfWhere(context.whereExpr);
        }

        public override object VisitGroupByClause([NotNull] PostgresParser.GroupByClauseContext context)
        {
            var res = new GroupByClause();
            foreach (var groupByItem in context.groupByItem())
                res.GroupColumns.Add((ColumnRef)Visit(groupByItem));
            return res;
        }

        public override object VisitGroupByItem([NotNull] PostgresParser.GroupByItemContext context)
        {
            return Visit(context.expression());
        }

        public override object VisitLimitClause([NotNull] PostgresParser.LimitClauseContext context)
        {
            return (uint?)((IntConstant)Visit(context.intLiteral())).intvalue;
        }

        public WhereClause ExpressionToCnfWhere(PostgresParser.ExpressionContext context)
        {
            var res = new WhereClause();
            var expr = (Expression)Visit(context);
            while (!CnfConverter.CheckCnf(expr)) expr = CnfConverter.ConvertToCnf(expr);
            res.CNF = CnfConverter.BuildCnf(expr);
            return res;
        }
    }
}
