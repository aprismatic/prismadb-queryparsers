using System;
using System.Collections.Generic;
using Irony.Parsing;
using PrismaDB.Commons;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DML;

namespace PrismaDB.QueryParser.MSSQL
{
    public partial class MsSqlParser
    {
        /// <summary>
        ///     Builds a Insert Query.
        /// </summary>
        /// <param name="insQuery">Resulting InsertQuery object</param>
        /// <param name="node">Parent node of query</param>
        private static void BuildInsertQuery(InsertQuery insQuery, ParseTreeNode node)
        {
            foreach (var mainNode in node.ChildNodes)
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                {
                    insQuery.Into = BuildTableRef(mainNode);
                }
                // Check for columns to update
                else if (mainNode.Term.Name.Equals("idlistPar"))
                {
                    var listNode = FindChildNode(mainNode, "idlist");
                    if (listNode != null)
                        foreach (var idNode in listNode.ChildNodes)
                            insQuery.Columns.Add(BuildColumnRef(idNode));
                }
                // Check for data to update
                else if (mainNode.Term.Name.Equals("insertDataList"))
                {
                    foreach (var dataNode in mainNode.ChildNodes)
                    {
                        var listNode = FindChildNode(dataNode, "exprList");
                        insQuery.Values.Add(BuildExpressions(listNode));
                    }
                }
        }


        /// <summary>
        ///     Builds a Update Query.
        /// </summary>
        /// <param name="updQuery">Resulting UpdateQuery object</param>
        /// <param name="node">Parent node of query</param>
        private static void BuildUpdateQuery(UpdateQuery updQuery, ParseTreeNode node)
        {
            foreach (var mainNode in node.ChildNodes)
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                {
                    updQuery.UpdateTable = BuildTableRef(mainNode);
                }
                // Check for columns and data to update
                else if (mainNode.Term.Name.Equals("assignList"))
                {
                    foreach (var exprNode in mainNode.ChildNodes)
                        if (exprNode.Term.Name.Equals("assignment"))
                        {
                            var colRef = BuildColumnRef(FindChildNode(exprNode, "Id"));
                            Constant constant = null;
                            if (FindChildNode(exprNode, "number") != null)
                                constant = new IntConstant(
                                    Convert.ToInt32(FindChildNode(exprNode, "number").Token.ValueString));
                            else if (FindChildNode(exprNode, "string") != null)
                                constant = new StringConstant(FindChildNode(exprNode, "string").Token.ValueString);

                            updQuery.UpdateExpressions.Add(new Pair<ColumnRef, Constant>(colRef, constant));
                        }
                }
                // Check for where clause
                else if (mainNode.Term.Name.Equals("whereClauseOpt"))
                {
                    updQuery.Where = BuildWhereClause(mainNode);
                }
        }


        /// <summary>
        ///     Builds a Delete Query.
        /// </summary>
        /// <param name="delQuery">Resulting DeleteQuery object</param>
        /// <param name="node">Parent node of query</param>
        private static void BuildDeleteQuery(DeleteQuery delQuery, ParseTreeNode node)
        {
            foreach (var mainNode in node.ChildNodes)
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                    delQuery.DeleteTable = BuildTableRef(mainNode);
                // Check and build where clause
                else if (mainNode.Term.Name.Equals("whereClauseOpt")) delQuery.Where = BuildWhereClause(mainNode);
        }


        /// <summary>
        ///     Builds a Select Query.
        /// </summary>
        /// <param name="selQuery">Resulting SelectQuery object</param>
        /// <param name="node">Parent node of query</param>
        private static void BuildSelectQuery(SelectQuery selQuery, ParseTreeNode node, string source)
        {
            foreach (var mainNode in node.ChildNodes)
                // Check and build columns to select from
                if (mainNode.Term.Name.Equals("selList"))
                {
                    var listNode = FindChildNode(mainNode, "columnItemList");

                    if (listNode != null)
                        foreach (var columnNode in listNode.ChildNodes)
                        {
                            // Find expression and column name nodes
                            var sourceNode = FindChildNode(columnNode, "columnSource");
                            var idNode = FindChildNode(columnNode, "Id");

                            foreach (var exprNode in sourceNode.ChildNodes)
                            {
                                // Build expression
                                var expr = BuildExpression(exprNode);
                                if (idNode != null)
                                    // Set alias
                                    expr.ColumnName = BuildColumnRef(idNode).ColumnName;
                                else if (expr.GetType() != typeof(ColumnRef))
                                    // Set original expression
                                    expr.ColumnName = new Identifier(source.Substring(
                                        exprNode.Span.EndPosition - exprNode.Span.Length, exprNode.Span.Length));

                                selQuery.SelectExpressions.Add(expr);
                            }
                        }
                }
                // Check table to select from
                else if (mainNode.Term.Name.Equals("fromClauseOpt"))
                {
                    var listNode = FindChildNode(mainNode, "idlist");

                    if (listNode != null)
                        foreach (var idNode in listNode.ChildNodes)
                            selQuery.FromTables.Add(BuildTableRef(idNode));
                }
                // Check and build where clause
                else if (mainNode.Term.Name.Equals("whereClauseOpt"))
                {
                    selQuery.Where = BuildWhereClause(mainNode);
                }
                // Check for TOP
                else if (mainNode.Term.Name.Equals("selRestrOpt"))
                {
                    if (FindChildNode(mainNode, "TOP") != null)
                        selQuery.Limit = Convert.ToUInt32(FindChildNode(mainNode, "number").Token.Value);
                }
                // Check for ORDER BY
                else if (mainNode.Term.Name.Equals("orderClauseOpt"))
                {
                    selQuery.OrderBy = BuildOrderByClause(mainNode);
                }
        }


        /// <summary>
        ///     Builds Where Clause.
        /// </summary>
        /// <param name="node">Parent node of clause</param>
        /// <returns>Resulting Where clause</returns>
        private static WhereClause BuildWhereClause(ParseTreeNode node)
        {
            var where = new WhereClause();
            Expression expr = null;
            foreach (var exprNode in node.ChildNodes)
            {
                // Build expression tree from Irony tree
                var tempExpr = BuildExpression(exprNode);
                if (tempExpr != null)
                {
                    expr = tempExpr;
                    break;
                }
            }

            // Converts expression tree to CNF iteratively until it is in full CNF form
            while (!CheckCNF(expr)) expr = ConvertToCNF(expr);

            // Build where clause from expression tree
            where.CNF = BuildCNF(expr);

            return where;
        }


        /// <summary>
        ///     Builds Order By clause.
        /// </summary>
        /// <param name="node">Parent node of clause</param>
        /// <returns>Resulting Order By clause</returns>
        private static OrderByClause BuildOrderByClause(ParseTreeNode node)
        {
            var orderBy = new OrderByClause();

            var listNode = FindChildNode(node, "orderList");

            if (listNode != null)
                foreach (var orderMemberNode in listNode.ChildNodes)
                {
                    var cofRef = BuildColumnRef(FindChildNode(orderMemberNode, "Id"));

                    var direction = OrderDirection.ASC;
                    var dirNode = FindChildNode(orderMemberNode, "orderDirOpt");
                    if (FindChildNode(dirNode, "DESC") != null)
                        direction = OrderDirection.DESC;

                    orderBy.OrderColumns.Add(new Tuple<ColumnRef, OrderDirection>(cofRef, direction));
                }

            return orderBy;
        }


        /// <summary>
        ///     Build list of Expressions.
        /// </summary>
        /// <param name="node">Parent node of expressions</param>
        /// <returns>List of Expressions</returns>
        private static List<Expression> BuildExpressions(ParseTreeNode node)
        {
            var exprs = new List<Expression>();

            if (node != null)
                foreach (var exprNode in node.ChildNodes)
                    // Build individual expressions and add to list
                    exprs.Add(BuildExpression(exprNode));
            return exprs;
        }


        /// <summary>
        ///     Build Expression tree recursively.
        /// </summary>
        /// <param name="node">Parent node of expression</param>
        /// <returns>Expression tree node</returns>
        private static Expression BuildExpression(ParseTreeNode node)
        {
            Expression expr = null;
            if (node != null)
            {
                if (node.Term.Name.Equals("Id"))
                {
                    expr = BuildColumnRef(node);
                }
                else if (node.Term.Name.Equals("funCall"))
                {
                    expr = BuildScalarFunction(node);
                }
                else if (node.Term.Name.Equals("string"))
                {
                    expr = new StringConstant(node.Token.ValueString);
                }
                else if (node.Term.Name.Equals("number"))
                {
                    int integer;
                    if (int.TryParse(node.Token.ValueString, out integer))
                        expr = new IntConstant(integer);
                    else
                        expr = new FloatingPointConstant(Convert.ToDouble(node.Token.Value));
                }
                else if (node.Term.Name.Equals("binExpr"))
                {
                    var opNode = FindChildNode(node, "binOp");
                    if (opNode != null)
                        if (node.ChildNodes.Count == 3)
                        {
                            if (FindChildNode(opNode, "+") != null)
                                expr = new Addition(BuildExpression(node.ChildNodes[0]),
                                    BuildExpression(node.ChildNodes[2]));
                            else if (FindChildNode(opNode, "*") != null)
                                expr = new Multiplication(BuildExpression(node.ChildNodes[0]),
                                    BuildExpression(node.ChildNodes[2]));
                            else if (FindChildNode(opNode, "=") != null)
                                expr = new BooleanEquals(BuildExpression(node.ChildNodes[0]),
                                    BuildExpression(node.ChildNodes[2]));
                            else if (FindChildNode(opNode, "!=") != null)
                                expr = new BooleanEquals(BuildExpression(node.ChildNodes[0]),
                                    BuildExpression(node.ChildNodes[2]), true);
                            else if (FindChildNode(opNode, "AND") != null)
                                expr = new AndClause(BuildExpression(node.ChildNodes[0]),
                                    BuildExpression(node.ChildNodes[2]));
                            else if (FindChildNode(opNode, "OR") != null)
                                expr = new OrClause(BuildExpression(node.ChildNodes[0]),
                                    BuildExpression(node.ChildNodes[2]));
                        }
                }
                else if (node.Term.Name.Equals("exprList"))
                {
                    if (node.ChildNodes.Count == 1) expr = BuildExpression(node.ChildNodes[0]);
                }
            }

            return expr;
        }
    }
}