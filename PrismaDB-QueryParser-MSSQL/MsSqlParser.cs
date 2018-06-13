using System;
using System.Collections.Generic;
using Irony.Parsing;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DCL;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;

namespace PrismaDB.QueryParser.MSSQL
{
    /// <summary>
    ///     Parses SQL statment strings into PrismaDB Query AST.
    /// </summary>
    public partial class MsSqlParser
    {
        private readonly Grammar _grammar = new MsSqlGrammar();
        private LanguageData _language;
        private Parser _parser;


        /// <summary>
        ///     Parses a SQL string to a list of PrismaDB Query objects.
        /// </summary>
        /// <param name="source">SQL query string</param>
        /// <returns>List of Query objects</returns>
        public List<Query> ParseToAst(string source)
        {
            // Declare list of queries
            var queries = new List<Query>();
            var parseTree = Parse(source);
            var node = parseTree.Root;

            try
            {
                if (node != null)
                    if (node.Term.Name.Equals("stmtList"))
                        foreach (var stmtNode in node.ChildNodes)
                            if (stmtNode.Term.Name.Equals("selectStmt"))
                            {
                                var selQuery = new SelectQuery();
                                BuildSelectQuery(selQuery, stmtNode, source);
                                queries.Add(selQuery);
                            }

                            else if (stmtNode.Term.Name.Equals("insertStmt"))
                            {
                                var insQuery = new InsertQuery();
                                BuildInsertQuery(insQuery, stmtNode);
                                queries.Add(insQuery);
                            }

                            else if (stmtNode.Term.Name.Equals("updateStmt"))
                            {
                                var updQuery = new UpdateQuery();
                                BuildUpdateQuery(updQuery, stmtNode);
                                queries.Add(updQuery);
                            }

                            else if (stmtNode.Term.Name.Equals("deleteStmt"))
                            {
                                var delQuery = new DeleteQuery();
                                BuildDeleteQuery(delQuery, stmtNode);
                                queries.Add(delQuery);
                            }

                            else if (stmtNode.Term.Name.Equals("createTableStmt"))
                            {
                                var createQuery = new CreateTableQuery();
                                BuildCreateTableQuery(createQuery, stmtNode);
                                queries.Add(createQuery);
                            }

                            else if (stmtNode.Term.Name.Equals("alterStmt"))
                            {
                                var alterQuery = new AlterTableQuery();
                                BuildAlterTableQuery(alterQuery, stmtNode);
                                queries.Add(alterQuery);
                            }

                            else if (stmtNode.Term.Name.Equals("exportSettingsCmd"))
                            {
                                var exportCommand =
                                    new ExportSettingsCommand(FindChildNode(stmtNode, "string").Token.ValueString);
                                queries.Add(exportCommand);
                            }

                            else if (stmtNode.Term.Name.Equals("useStmt"))
                            {
                                var delQuery = new UseStatement(BuildDatabaseRef(FindChildNode(stmtNode, "Id")));
                                queries.Add(delQuery);
                            }
            }
            catch (ApplicationException)
            {
                return new List<Query>();
            }

            return queries;
        }


        /// <summary>
        ///     Parses a SQL string to Irony ParseTree object.
        /// </summary>
        /// <param name="source">SQL query string</param>
        /// <returns>Irony ParseTree object</returns>
        private ParseTree Parse(string source)
        {
            _language = new LanguageData(_grammar);
            _parser = new Parser(_language);

            if (_parser == null || !_parser.Language.CanParse())
                return null;

            _parser.Parse(source);
            var parseTree = _parser.Context.CurrentParseTree;

            return parseTree;
        }


        /// <summary>
        ///     Finds a specific immediate child node. Returns null if not found.
        /// </summary>
        /// <param name="parentNode">The parent node</param>
        /// <param name="termName">The name of the child node to find</param>
        /// <returns>The child node</returns>
        private static ParseTreeNode FindChildNode(ParseTreeNode parentNode, string termName, int occurrence = 0)
        {
            if (parentNode == null) return null;

            foreach (var childNode in parentNode.ChildNodes)
                if (childNode.Term.Name.Equals(termName))
                    if (occurrence-- == 0)
                        return childNode;
            return null;
        }


        /// <summary>
        ///     Build table reference with table name.
        /// </summary>
        /// <param name="node">Parent node of table</param>
        /// <returns>Table reference</returns>
        private static TableRef BuildTableRef(ParseTreeNode node)
        {
            TableRef exp = null;

            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("id_simple"))
                exp = new TableRef(node.ChildNodes[0].Token.ValueString);
            return exp;
        }


        /// <summary>
        ///     Build column reference with column name.
        /// </summary>
        /// <param name="node">Parent node of column</param>
        /// <returns>Column reference</returns>
        private static ColumnRef BuildColumnRef(ParseTreeNode node)
        {
            ColumnRef exp = null;

            // Without table name
            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("id_simple"))
                exp = new ColumnRef(node.ChildNodes[0].Token.ValueString);
            // With table name
            else if (node.ChildNodes.Count == 2 && node.ChildNodes[0].Term.Name.Equals("id_simple") &&
                     node.ChildNodes[1].Term.Name.Equals("id_simple"))
                exp = new ColumnRef(node.ChildNodes[0].Token.ValueString, node.ChildNodes[1].Token.ValueString);
            return exp;
        }


        /// <summary>
        ///     Build database reference with database name.
        /// </summary>
        /// <param name="node">Parent node of database</param>
        /// <returns>Database reference</returns>
        private static DatabaseRef BuildDatabaseRef(ParseTreeNode node)
        {
            DatabaseRef exp = null;

            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("id_simple"))
                exp = new DatabaseRef(node.ChildNodes[0].Token.ValueString);
            return exp;
        }


        /// <summary>
        ///     Build ScalarFunction with parameters.
        /// </summary>
        /// <param name="node">Parent node of ScalarFunction</param>
        /// <returns>Function call</returns>
        private static Expression BuildScalarFunction(ParseTreeNode node)
        {
            Expression exp = null;

            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("exprList"))
            {
                exp = BuildExpression(node.ChildNodes[0]);
            }
            else if (node.ChildNodes.Count == 2 &&
                     (node.ChildNodes[0].Term.Name.Equals("Id") &&
                      node.ChildNodes[1].Term.Name.Equals("funArgs")))
            {
                if (node.ChildNodes[0].ChildNodes.Count == 1 &&
                    node.ChildNodes[0].ChildNodes[0].Term.Name.Equals("id_simple"))
                {
                    var funcName = node.ChildNodes[0].ChildNodes[0].Token.ValueString.ToUpperInvariant();
                    switch (funcName)
                    {
                        case "SUM":
                            exp = new SumAggregationFunction(funcName);
                            break;
                        case "AVG":
                            exp = new AvgAggregationFunction(funcName);
                            break;
                        case "COUNT":
                            exp = new CountAggregationFunction(funcName);
                            break;
                        default:
                            exp = new ScalarFunction(funcName);
                            break;
                    }
                }

                if (node.ChildNodes[1].ChildNodes.Count == 1 &&
                    node.ChildNodes[1].ChildNodes[0].Term.Name.Equals("exprList"))
                    ((ScalarFunction)exp).Parameters = BuildExpressions(node.ChildNodes[1].ChildNodes[0]);
            }
            else if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("CURRENT_TIMESTAMP"))
            {
                exp = new ScalarFunction(node.ChildNodes[0].Token.ValueString);
            }

            return exp;
        }
    }
}