using Irony.Parsing;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryAST.Misc;
using System;
using System.Collections.Generic;


namespace PrismaDB.QueryParser
{
    public class SqlParser
    {
        Grammar grammar = new SqlGrammar();
        LanguageData language;
        Parser parser;

        public List<Query> ParseToAST(string source)
        {
            List<Query> queries = new List<Query>();
            ParseTree parseTree = Parse(source);
            ParseTreeNode node = parseTree.Root;

            if (node != null)
            {
                if (node.Term.Name.Equals("stmtList"))
                {
                    foreach (ParseTreeNode stmtNode in node.ChildNodes)
                    {
                        if (stmtNode.Term.Name.Equals("selectStmt"))
                        {
                            SelectQuery selQuery = new SelectQuery();
                            BuildSelectQuery(selQuery, stmtNode);
                            queries.Add(selQuery);
                        }

                        else if (stmtNode.Term.Name.Equals("insertStmt"))
                        {
                            InsertQuery insQuery = new InsertQuery();
                            BuildInsertQuery(insQuery, stmtNode);
                            queries.Add(insQuery);
                        }

                        else if (stmtNode.Term.Name.Equals("updateStmt"))
                        {
                            UpdateQuery updQuery = new UpdateQuery();
                            BuildUpdateQuery(updQuery, stmtNode);
                            queries.Add(updQuery);
                        }

                        else if (stmtNode.Term.Name.Equals("deleteStmt"))
                        {
                            DeleteQuery delQuery = new DeleteQuery();
                            BuildDeleteQuery(delQuery, stmtNode);
                            queries.Add(delQuery);
                        }

                        else if (stmtNode.Term.Name.Equals("createTableStmt"))
                        {
                            CreateTableQuery createQuery = new CreateTableQuery();

                            BuildCreateTableQuery(createQuery, stmtNode);

                            queries.Add(createQuery);
                        }
                    }
                }
            }
            return queries;
        }

        private ParseTree Parse(string source)
        {
            language = new LanguageData(grammar);
            parser = new Parser(language);

            ParseTree parseTree = null;

            if (parser == null || !parser.Language.CanParse()) return null;
            parseTree = null;
            GC.Collect();
            try
            {
                parser.Parse(source);
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                parseTree = parser.Context.CurrentParseTree;
            }
            return parseTree;
        }

        private void BuildCreateTableQuery(CreateTableQuery createQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                if (mainNode.Term.Name.Equals("Id"))
                {
                    createQuery.TableName = BuildTableRef(mainNode);
                }
                else if (mainNode.Term.Name.Equals("fieldDefList"))
                {
                    foreach (ParseTreeNode fieldDefNode in mainNode.ChildNodes)
                    {
                        ColumnDefinition colDef = new ColumnDefinition();
                        colDef.ColumnName = BuildColumnRef(FindChildNode(fieldDefNode, "Id")).ColumnName;

                        ParseTreeNode dataTypeNode = FindChildNode(fieldDefNode, "typeName");
                        if (FindChildNode(dataTypeNode, "int") != null)
                        {
                            colDef.DataType = SQLDataType.INT;
                        }
                        else if (FindChildNode(dataTypeNode, "varchar") != null)
                        {
                            colDef.DataType = SQLDataType.VARCHAR;
                        }
                        else if (FindChildNode(dataTypeNode, "uniqueidentifier") != null)
                        {
                            colDef.DataType = SQLDataType.UNIQUEIDENTIFIER;
                        }
                        else if (FindChildNode(dataTypeNode, "varbinary") != null)
                        {
                            colDef.DataType = SQLDataType.VARBINARY;
                        }

                        ParseTreeNode paraNode = FindChildNode(FindChildNode(fieldDefNode, "typeParams"), "number");
                        if (paraNode != null) colDef.Length = Convert.ToInt32(paraNode.Token.ValueString);

                        colDef.Nullable = CheckNull(FindChildNode(fieldDefNode, "nullSpecOpt"));

                        ParseTreeNode newidNode = FindChildNode(FindChildNode(fieldDefNode, "newidOpt"), "DEFAULT NEWID()");
                        if (newidNode != null) colDef.isRowId = true;

                        if (paraNode != null) colDef.Length = Convert.ToInt32(paraNode.Token.ValueString);
                        createQuery.ColumnDefinitions.Add(colDef);
                    }
                }
            }
        }



        private void BuildDeleteQuery(DeleteQuery delQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                if (mainNode.Term.Name.Equals("Id"))
                {
                    delQuery.DeleteTable = BuildTableRef(mainNode);
                }
                else if (mainNode.Term.Name.Equals("whereClauseOpt"))
                {
                    delQuery.Where = BuildWhereClause(mainNode);
                }
            }
        }


        private void BuildSelectQuery(SelectQuery selQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                if (mainNode.Term.Name.Equals("selList"))
                {
                    ParseTreeNode listNode = FindChildNode(mainNode, "columnItemList");

                    if (listNode != null)
                    {
                        foreach (ParseTreeNode columnNode in listNode.ChildNodes)
                        {
                            ParseTreeNode sourceNode = FindChildNode(columnNode, "columnSource");
                            ParseTreeNode idNode = FindChildNode(columnNode, "Id");

                            foreach (ParseTreeNode exprNode in sourceNode.ChildNodes)
                            {
                                Expression expr = BuildExpression(exprNode);
                                if (idNode != null)
                                {
                                    expr.ColumnName = BuildColumnRef(idNode).ColumnName;
                                }
                                selQuery.SelectExpressions.Add(expr);
                            }
                        }
                    }
                }
                else if (mainNode.Term.Name.Equals("fromClauseOpt"))
                {
                    ParseTreeNode listNode = FindChildNode(mainNode, "idlist");

                    if (listNode != null)
                    {
                        foreach (ParseTreeNode idNode in listNode.ChildNodes)
                        {
                            selQuery.FromTables.Add(BuildTableRef(idNode));
                        }
                    }
                }
                else if (mainNode.Term.Name.Equals("whereClauseOpt"))
                {
                    selQuery.Where = BuildWhereClause(mainNode);
                }
            }
        }

        private WhereClause BuildWhereClause(ParseTreeNode node)
        {
            WhereClause where = new WhereClause();
            Expression expr = null;
            foreach (ParseTreeNode exprNode in node.ChildNodes)
            {
                Expression tempExpr = BuildExpression(exprNode);
                if (tempExpr != null)
                {
                    expr = tempExpr;
                    break;
                }
            }

            while (!CheckCNF(expr))
            {
                expr = ConvertToCNF(expr);
            }


            where.CNF = BuildCNF(expr);

            return where;
        }


        private Boolean CheckCNF(Expression expr)
        {
            if (expr.GetType() == typeof(OrClause))
            {
                OrClause or = (OrClause)expr;
                if (or.left.GetType() == typeof(AndClause))
                {
                    return false;
                }
                else if (or.right.GetType() == typeof(AndClause))
                {
                    return false;
                }
                else
                {
                    return (CheckCNF(or.left) && CheckCNF(or.right));
                }
            }
            else if (expr.GetType() == typeof(AndClause))
            {
                AndClause and = (AndClause)expr;
                return (CheckCNF(and.left) && CheckCNF(and.right));
            }
            else
            {
                return true;
            }
        }


        private ConjunctiveNormalForm BuildCNF(Expression expr)
        {
            ConjunctiveNormalForm cnf = new ConjunctiveNormalForm();

            if (expr != null)
            {
                if (expr.GetType() == typeof(AndClause))
                {
                    cnf.AND.AddRange(BuildCNF(((AndClause)expr).left).AND);
                    cnf.AND.AddRange(BuildCNF(((AndClause)expr).right).AND);
                }
                else
                {
                    cnf.AND.Add(BuildDisjunction(expr));
                }
            }
            return cnf;
        }


        private Disjunction BuildDisjunction(Expression expr)
        {
            Disjunction disjunction = new Disjunction();
            if (expr != null)
            {
                if (expr.GetType() == typeof(OrClause))
                {
                    disjunction.OR.AddRange(BuildDisjunction(((OrClause)expr).left).OR);
                    disjunction.OR.AddRange(BuildDisjunction(((OrClause)expr).right).OR);
                }
                if (expr.GetType() == typeof(BooleanEquals))
                {
                    disjunction.OR.Add((BooleanEquals)expr);
                }
            }
            return disjunction;
        }


        private Expression ConvertToCNF(Expression expr)
        {
            if (expr.GetType() == typeof(OrClause))
            {
                OrClause or = (OrClause)expr;
                if (or.left.GetType() == typeof(AndClause))
                {
                    AndClause childAnd = (AndClause)or.left;

                    Expression q = childAnd.left.Clone();
                    Expression r = childAnd.right.Clone();
                    Expression p = or.right.Clone();

                    AndClause newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
                    newAnd.left = ConvertToCNF(newAnd.left);
                    newAnd.right = ConvertToCNF(newAnd.right);
                    return newAnd;
                }
                else if (or.right.GetType() == typeof(AndClause))
                {
                    AndClause childAnd = (AndClause)or.right;

                    Expression q = childAnd.left.Clone();
                    Expression r = childAnd.right.Clone();
                    Expression p = or.left.Clone();

                    AndClause newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
                    newAnd.left = ConvertToCNF(newAnd.left);
                    newAnd.right = ConvertToCNF(newAnd.right);
                    return newAnd;
                }
                else
                {
                    or.left = ConvertToCNF(or.left);
                    or.right = ConvertToCNF(or.right);
                    return or;
                }
            }
            else if (expr.GetType() == typeof(AndClause))
            {
                AndClause and = (AndClause)expr;
                and.left = ConvertToCNF(and.left);
                and.right = ConvertToCNF(and.right);
                return and;
            }
            else
            {
                return expr;
            }
        }


        private void BuildUpdateQuery(UpdateQuery updQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                if (mainNode.Term.Name.Equals("Id"))
                {
                    updQuery.UpdateTable = BuildTableRef(mainNode);
                }
                else if (mainNode.Term.Name.Equals("assignList"))
                {
                    foreach (ParseTreeNode exprNode in mainNode.ChildNodes)
                    {
                        if (exprNode.Term.Name.Equals("assignment"))
                        {
                            ColumnRef colRef = BuildColumnRef(FindChildNode(exprNode, "Id"));
                            Constant constant = null;
                            if (FindChildNode(exprNode, "number") != null)
                            {
                                constant = new IntConstant(Convert.ToInt32(FindChildNode(exprNode, "number").Token.ValueString));
                            }
                            else if (FindChildNode(exprNode, "string") != null)
                            {
                                constant = new StringConstant(FindChildNode(exprNode, "string").Token.ValueString);
                            }

                            updQuery.UpdateExpressions.Add(new Pair<ColumnRef, Constant>(colRef, constant));
                        }
                    }
                }
                else if (mainNode.Term.Name.Equals("whereClauseOpt"))
                {
                    updQuery.Where = BuildWhereClause(mainNode);
                }
            }
        }

        private void BuildInsertQuery(InsertQuery insQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                if (mainNode.Term.Name.Equals("Id"))
                {
                    insQuery.Into = BuildTableRef(mainNode);
                }
                else if (mainNode.Term.Name.Equals("idlistPar"))
                {
                    ParseTreeNode listNode = FindChildNode(mainNode, "idlist");
                    if (listNode != null)
                    {
                        foreach (ParseTreeNode idNode in listNode.ChildNodes)
                        {
                            insQuery.Columns.Add(BuildColumnRef(idNode));
                        }
                    }
                }
                else if (mainNode.Term.Name.Equals("insertDataList"))
                {
                    foreach (ParseTreeNode dataNode in mainNode.ChildNodes)
                    {
                        ParseTreeNode listNode = FindChildNode(dataNode, "exprList");
                        insQuery.Values.Add(BuildExpressions(listNode));
                    }
                }
            }
        }


        private ParseTreeNode FindChildNode(ParseTreeNode parentNode, string termName)
        {
            if (parentNode == null) return null;

            foreach (ParseTreeNode childNode in parentNode.ChildNodes)
            {
                if (childNode.Term.Name.Equals(termName))
                {
                    return childNode;
                }
            }
            return null;
        }



        private List<Expression> BuildExpressions(ParseTreeNode node)
        {
            List<Expression> exprs = new List<Expression>();

            if (node != null)
            {
                foreach (ParseTreeNode exprNode in node.ChildNodes)
                {
                    exprs.Add(BuildExpression(exprNode));
                }
            }
            return exprs;
        }

        private Expression BuildExpression(ParseTreeNode node)
        {
            Expression expr = null;
            if (node != null)
            {
                if (node.Term.Name.Equals("Id"))
                {
                    expr = BuildColumnRef(node);
                }
                else if (node.Term.Name.Equals("string"))
                {
                    expr = new StringConstant(node.Token.ValueString);
                }
                else if (node.Term.Name.Equals("number"))
                {
                    expr = new IntConstant(Convert.ToInt32(node.Token.ValueString));
                }
                else if (node.Term.Name.Equals("binExpr"))
                {
                    ParseTreeNode opNode = FindChildNode(node, "binOp");
                    if (opNode != null)
                    {
                        if (node.ChildNodes.Count == 3)
                        {
                            if (FindChildNode(opNode, "+") != null)
                            {
                                expr = new Addition(BuildExpression(node.ChildNodes[0]), BuildExpression(node.ChildNodes[2]));
                            }
                            else if (FindChildNode(opNode, "*") != null)
                            {
                                expr = new Multiplication(BuildExpression(node.ChildNodes[0]), BuildExpression(node.ChildNodes[2]));
                            }
                            else if (FindChildNode(opNode, "=") != null)
                            {
                                expr = new BooleanEquals(BuildExpression(node.ChildNodes[0]), BuildExpression(node.ChildNodes[2]));
                            }
                            else if (FindChildNode(opNode, "!=") != null)
                            {
                                expr = new BooleanEquals(BuildExpression(node.ChildNodes[0]), BuildExpression(node.ChildNodes[2]), true);
                            }
                            else if (FindChildNode(opNode, "AND") != null)
                            {
                                expr = new AndClause(BuildExpression(node.ChildNodes[0]), BuildExpression(node.ChildNodes[2]));
                            }
                            else if (FindChildNode(opNode, "OR") != null)
                            {
                                expr = new OrClause(BuildExpression(node.ChildNodes[0]), BuildExpression(node.ChildNodes[2]));
                            }
                        }
                    }
                }
                else if (node.Term.Name.Equals("exprList"))
                {
                    if (node.ChildNodes.Count == 1)
                    {
                        expr = BuildExpression(node.ChildNodes[0]);
                    }
                }
            }
            return expr;
        }



        public Boolean CheckNull(ParseTreeNode node)
        {
            if (node != null)
            {
                if (node.ChildNodes.Count > 1)
                {
                    if (node.ChildNodes[0].Token.ValueString.Equals("not") && node.ChildNodes[1].Token.ValueString.Equals("null"))
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        private ColumnRef BuildColumnRef(ParseTreeNode node)
        {
            ColumnRef exp = null;

            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("id_simple"))
            {
                exp = new ColumnRef(node.ChildNodes[0].Token.ValueString);
            }
            else if (node.ChildNodes.Count == 2 && node.ChildNodes[0].Term.Name.Equals("id_simple") && node.ChildNodes[1].Term.Name.Equals("id_simple"))
            {
                exp = new ColumnRef(node.ChildNodes[0].Token.ValueString, node.ChildNodes[1].Token.ValueString);
            }
            return exp;
        }

        private TableRef BuildTableRef(ParseTreeNode node)
        {
            TableRef exp = null;

            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("id_simple"))
            {
                exp = new TableRef(node.ChildNodes[0].Token.ValueString);
            }
            return exp;
        }

    }
}
