using Irony.Parsing;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.Commons;
using System;
using System.Collections.Generic;


namespace PrismaDB.QueryParser
{
    /// <summary>
    /// Parses SQL statment strings into PrismaDB Query AST.
    /// </summary>
    public class SqlParser
    {
        Grammar grammar = new SqlGrammar();
        LanguageData language;
        Parser parser;


        /// <summary>
        /// Parses a SQL string to a list of PrismaDB Query objects.
        /// </summary>
        /// <param name="source">SQL query string</param>
        /// <returns>List of Query objects</returns>
        public List<Query> ParseToAST(string source)
        {
            // Declare list of queries
            var queries = new List<Query>();
            var parseTree = Parse(source);
            var node = parseTree.Root;

            if (node != null)
            {
                // Check root node for parsed statements
                if (node.Term.Name.Equals("stmtList"))
                {
                    // Loop through child nodes for various SQL statements and build accordingly
                    // Add query into into list
                    foreach (ParseTreeNode stmtNode in node.ChildNodes)
                    {
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

                        else if (stmtNode.Term.Name.Equals("useStmt"))
                        {
                            throw new NotSupportedException("Database switching not supported.");
                        }
                    }
                }
            }
            return queries;
        }


        /// <summary>
        /// Parses a SQL string to Irony ParseTree object.
        /// </summary>
        /// <param name="source">SQL query string</param>
        /// <returns>Irony ParseTree object</returns>
        private ParseTree Parse(string source)
        {
            language = new LanguageData(grammar);
            parser = new Parser(language);

            if (parser == null || !parser.Language.CanParse())
                return null;

            parser.Parse(source);
            var parseTree = parser.Context.CurrentParseTree;

            return parseTree;
        }


        /// <summary>
        /// Builds a Create Table Query.
        /// </summary>
        /// <param name="createQuery">Resulting CreateTableQuery object</param>
        /// <param name="node">Parent node of query</param>
        private void BuildCreateTableQuery(CreateTableQuery createQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                {
                    createQuery.TableName = BuildTableRef(mainNode);
                }
                // Check for columns
                else if (mainNode.Term.Name.Equals("fieldDefList"))
                {
                    foreach (ParseTreeNode fieldDefNode in mainNode.ChildNodes)
                    {
                        createQuery.ColumnDefinitions.Add(BuildColumnDefinition(fieldDefNode));
                    }
                }
            }
        }


        /// <summary>
        /// Builds a Alter Table Query.
        /// </summary>
        /// <param name="alterQuery">Resulting AlterTableQuery object</param>
        /// <param name="node">Parent node of query</param>
        private void BuildAlterTableQuery(AlterTableQuery alterQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                {
                    alterQuery.TableName = BuildTableRef(mainNode);
                }
                // Check for column
                else if (mainNode.Term.Name.Equals("alterCmd"))
                {
                    // Only ALTER COLUMN is supported now
                    alterQuery.AlterType = AlterType.MODIFY;
                    alterQuery.ColumnDefinitions.Add(BuildColumnDefinition(FindChildNode(mainNode, "fieldDef")));
                }
            }
        }


        /// <summary>
        /// Builds a Delete Query.
        /// </summary>
        /// <param name="delQuery">Resulting DeleteQuery object</param>
        /// <param name="node">Parent node of query</param>
        private void BuildDeleteQuery(DeleteQuery delQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                {
                    delQuery.DeleteTable = BuildTableRef(mainNode);
                }
                // Check and build where clause
                else if (mainNode.Term.Name.Equals("whereClauseOpt"))
                {
                    delQuery.Where = BuildWhereClause(mainNode);
                }
            }
        }


        /// <summary>
        /// Builds a Select Query.
        /// </summary>
        /// <param name="selQuery">Resulting SelectQuery object</param>
        /// <param name="node">Parent node of query</param>
        private void BuildSelectQuery(SelectQuery selQuery, ParseTreeNode node, string source)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                // Check and build columns to select from
                if (mainNode.Term.Name.Equals("selList"))
                {
                    ParseTreeNode listNode = FindChildNode(mainNode, "columnItemList");

                    if (listNode != null)
                    {
                        foreach (ParseTreeNode columnNode in listNode.ChildNodes)
                        {
                            // Find expression and column name nodes
                            ParseTreeNode sourceNode = FindChildNode(columnNode, "columnSource");
                            ParseTreeNode idNode = FindChildNode(columnNode, "Id");

                            foreach (ParseTreeNode exprNode in sourceNode.ChildNodes)
                            {
                                // Build expression
                                Expression expr = BuildExpression(exprNode);
                                if (idNode != null)
                                    // Set alias
                                    expr.ColumnName = BuildColumnRef(idNode).ColumnName;
                                else if (expr.GetType() != typeof(ColumnRef))
                                    // Set original expression
                                    expr.ColumnName = new Identifier(source.Substring(exprNode.Span.EndPosition - exprNode.Span.Length, exprNode.Span.Length));

                                selQuery.SelectExpressions.Add(expr);
                            }
                        }
                    }
                }
                // Check table to select from
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
            }
        }


        /// <summary>
        /// Builds Column Definition.
        /// </summary>
        /// <param name="node">Column Definition node</param>
        /// <returns>Resulting Column Definition</returns>
        private ColumnDefinition BuildColumnDefinition(ParseTreeNode node)
        {
            // Create and set name of column definition
            var colDef = new ColumnDefinition(BuildColumnRef(FindChildNode(node, "Id")).ColumnName);

            // Check for datatype
            ParseTreeNode dataTypeNode = FindChildNode(node, "typeName");
            if (FindChildNode(dataTypeNode, "INT") != null)
            {
                colDef.DataType = SQLDataType.INT;
            }
            else if (FindChildNode(dataTypeNode, "VARCHAR") != null)
            {
                colDef.DataType = SQLDataType.VARCHAR;
            }
            else if (FindChildNode(dataTypeNode, "UNIQUEIDENTIFIER") != null)
            {
                colDef.DataType = SQLDataType.MSSQL_UNIQUEIDENTIFIER;
            }
            else if (FindChildNode(dataTypeNode, "VARBINARY") != null)
            {
                colDef.DataType = SQLDataType.VARBINARY;
            }
            else if (FindChildNode(dataTypeNode, "TEXT") != null)
            {
                colDef.DataType = SQLDataType.TEXT;
            }
            else if (FindChildNode(dataTypeNode, "DATETIME") != null)
            {
                colDef.DataType = SQLDataType.DATETIME;
            }

            // Check for datatype length
            ParseTreeNode paraNode = FindChildNode(FindChildNode(node, "typeParams"), "number");
            if (paraNode != null) colDef.Length = Convert.ToInt32(paraNode.Token.ValueString);

            // Check for nullable
            colDef.Nullable = CheckNull(FindChildNode(node, "nullSpecOpt"));

            // Check for encryption
            colDef.EncryptionFlags = CheckEncryption(FindChildNode(node, "encryptionOpt"));

            // Check for row id
            ParseTreeNode newidNode = FindChildNode(FindChildNode(node, "newidOpt"), "DEFAULT NEWID()");
            if (newidNode != null) colDef.isRowId = true;

            return colDef;
        }


        /// <summary>
        /// Builds Where Clause.
        /// </summary>
        /// <param name="node">Parent node of clause</param>
        /// <returns>Resulting Where clause</returns>
        private WhereClause BuildWhereClause(ParseTreeNode node)
        {
            var where = new WhereClause();
            Expression expr = null;
            foreach (ParseTreeNode exprNode in node.ChildNodes)
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
            while (!CheckCNF(expr))
            {
                expr = ConvertToCNF(expr);
            }

            // Build where clause from expression tree
            where.CNF = BuildCNF(expr);

            return where;
        }


        /// <summary>
        /// Checks if expression tree is in full CNF form recursively.
        /// </summary>
        /// <param name="expr">Expression tree root</param>
        /// <returns>Whether expression tree is in CNF</returns>
        private bool CheckCNF(Expression expr)
        {
            // If expression is empty, it is in CNF
            // Or terminates if is a leaf 
            if (expr == null)
                return true;


            if (expr.GetType() == typeof(OrClause))
            {
                var or = (OrClause)expr;
                // If a child of an OrClause is an AndClause, it is not in CNF
                if (or.left.GetType() == typeof(AndClause))
                    return false;
                if (or.right.GetType() == typeof(AndClause))
                    return false;

                // Continue checking children (of children)
                return (CheckCNF(or.left) && CheckCNF(or.right));
            }

            if (expr.GetType() == typeof(AndClause))
            {
                // AndClause can have either OrClause or AndClause as children
                var and = (AndClause)expr;
                return (CheckCNF(and.left) && CheckCNF(and.right));
            }

            return true;
        }


        /// <summary>
        /// Build ConjunctiveNormalForm object from Expression tree.
        /// </summary>
        /// <param name="expr">Expression tree</param>
        /// <returns>ConjunctiveNormalForm object</returns>
        private ConjunctiveNormalForm BuildCNF(Expression expr)
        {
            var cnf = new ConjunctiveNormalForm();

            if (expr != null)
            {
                // If expression node is an AndClause, call BuildCNF recursively to add ANDs
                if (expr.GetType() == typeof(AndClause))
                {
                    cnf.AND.AddRange(BuildCNF(((AndClause)expr).left).AND);
                    cnf.AND.AddRange(BuildCNF(((AndClause)expr).right).AND);
                }
                else
                {
                    // If expression is an OrClause, build Disjunction and add to AND
                    cnf.AND.Add(BuildDisjunction(expr));
                }
            }
            return cnf;
        }


        /// <summary>
        /// Build Disjunction object from Expression tree.
        /// </summary>
        /// <param name="expr">OR Expression tree</param>
        /// <returns>Disjunction object</returns>
        private Disjunction BuildDisjunction(Expression expr)
        {
            var disjunction = new Disjunction();
            if (expr != null)
            {
                if (expr.GetType() == typeof(OrClause))
                {
                    // If expression has more OR children, call BuildDisjunction recursively to add ORs
                    disjunction.OR.AddRange(BuildDisjunction(((OrClause)expr).left).OR);
                    disjunction.OR.AddRange(BuildDisjunction(((OrClause)expr).right).OR);
                }
                if (expr.GetType() == typeof(BooleanEquals))
                {
                    // Boolean expression
                    disjunction.OR.Add((BooleanEquals)expr);
                }
            }
            return disjunction;
        }


        /// <summary>
        /// Converts Expression tree to CNF form recursively.
        /// May require several runs to reach full CNF form.
        /// </summary>
        /// <param name="expr">Expression tree node</param>
        /// <returns>Expression tree in CNF form</returns>
        private Expression ConvertToCNF(Expression expr)
        {
            // If a child of an OrClause is an AndClause
            // Convert to CNF using distributive law
            // And continue to call ConvertToCNF recursively for the children
            if (expr.GetType() == typeof(OrClause))
            {
                var or = (OrClause)expr;
                if (or.left.GetType() == typeof(AndClause))
                {
                    var childAnd = (AndClause)or.left;

                    var q = childAnd.left.Clone() as Expression;
                    var r = childAnd.right.Clone() as Expression;
                    var p = or.right.Clone() as Expression;

                    var newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
                    newAnd.left = ConvertToCNF(newAnd.left);
                    newAnd.right = ConvertToCNF(newAnd.right);
                    return newAnd;
                }
                else if (or.right.GetType() == typeof(AndClause))
                {
                    AndClause childAnd = (AndClause)or.right;

                    var q = childAnd.left.Clone() as Expression;
                    var r = childAnd.right.Clone() as Expression;
                    var p = or.left.Clone() as Expression;

                    var newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
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

            return expr;
        }


        /// <summary>
        /// Builds a Update Query.
        /// </summary>
        /// <param name="updQuery">Resulting UpdateQuery object</param>
        /// <param name="node">Parent node of query</param>
        private void BuildUpdateQuery(UpdateQuery updQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                {
                    updQuery.UpdateTable = BuildTableRef(mainNode);
                }
                // Check for columns and data to update
                else if (mainNode.Term.Name.Equals("assignList"))
                {
                    foreach (ParseTreeNode exprNode in mainNode.ChildNodes)
                    {
                        if (exprNode.Term.Name.Equals("assignment"))
                        {
                            var colRef = BuildColumnRef(FindChildNode(exprNode, "Id"));
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
                // Check for where clause
                else if (mainNode.Term.Name.Equals("whereClauseOpt"))
                {
                    updQuery.Where = BuildWhereClause(mainNode);
                }
            }
        }


        /// <summary>
        /// Builds a Insert Query.
        /// </summary>
        /// <param name="insQuery">Resulting InsertQuery object</param>
        /// <param name="node">Parent node of query</param>
        private void BuildInsertQuery(InsertQuery insQuery, ParseTreeNode node)
        {
            foreach (ParseTreeNode mainNode in node.ChildNodes)
            {
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
                    {
                        foreach (ParseTreeNode idNode in listNode.ChildNodes)
                        {
                            insQuery.Columns.Add(BuildColumnRef(idNode));
                        }
                    }
                }
                // Check for data to update
                else if (mainNode.Term.Name.Equals("insertDataList"))
                {
                    foreach (ParseTreeNode dataNode in mainNode.ChildNodes)
                    {
                        var listNode = FindChildNode(dataNode, "exprList");
                        insQuery.Values.Add(BuildExpressions(listNode));
                    }
                }
            }
        }


        /// <summary>
        /// Finds a specific immediate child node. Returns null if not found.
        /// </summary>
        /// <param name="parentNode">The parent node</param>
        /// <param name="termName">The name of the child node to find</param>
        /// <returns>The child node</returns>
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


        /// <summary>
        /// Build list of Expressions.
        /// </summary>
        /// <param name="node">Parent node of expressions</param>
        /// <returns>List of Expressions</returns>
        private List<Expression> BuildExpressions(ParseTreeNode node)
        {
            var exprs = new List<Expression>();

            if (node != null)
            {
                foreach (ParseTreeNode exprNode in node.ChildNodes)
                {
                    // Build individual expressions and add to list
                    exprs.Add(BuildExpression(exprNode));
                }
            }
            return exprs;
        }


        /// <summary>
        /// Build Expression tree recursively.
        /// </summary>
        /// <param name="node">Parent node of expression</param>
        /// <returns>Expression tree node</returns>
        private Expression BuildExpression(ParseTreeNode node)
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
                    expr = new IntConstant(Convert.ToInt32(node.Token.Value));
                }
                else if (node.Term.Name.Equals("binExpr"))
                {
                    ParseTreeNode opNode = FindChildNode(node, "binOp");
                    if (opNode != null)
                    {
                        // Calls BuildExpression recursively for binary expressions.
                        // Left and right nodes are at index 0 and 2 respectively.
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


        /// <summary>
        /// Check for NOT NULL.
        /// </summary>
        /// <param name="node">Parent node of query</param>
        /// <returns>True if nullable</returns>
        public bool CheckNull(ParseTreeNode node)
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


        /// <summary>
        /// Check for encryption schemes.
        /// </summary>
        /// <param name="node">Parent node of query</param>
        /// <returns>Column encryption enum flags</returns>
        public ColumnEncryptionFlags CheckEncryption(ParseTreeNode node)
        {
            if (node == null)
                return ColumnEncryptionFlags.None;

            ParseTreeNode encryptTypeParNode = FindChildNode(node, "encryptTypePar");
            if (encryptTypeParNode == null || FindChildNode(node, "ENCRYPTED") == null)
                return ColumnEncryptionFlags.None;

            ParseTreeNode encryptTypeNodes = FindChildNode(encryptTypeParNode, "encryptTypeList");
            if (encryptTypeNodes == null)
                return ColumnEncryptionFlags.Store;

            ColumnEncryptionFlags flags = ColumnEncryptionFlags.None;
            foreach (ParseTreeNode childNode in encryptTypeNodes.ChildNodes)
            {
                if (FindChildNode(childNode, "STORE") != null)
                {
                    flags |= ColumnEncryptionFlags.Store;
                }
                else if (FindChildNode(childNode, "INTEGER_ADDITION") != null)
                {
                    flags |= ColumnEncryptionFlags.IntegerAddition;
                }
                else if (FindChildNode(childNode, "INTEGER_MULTIPLICATION") != null)
                {
                    flags |= ColumnEncryptionFlags.IntegerMultiplication;
                }
                else if (FindChildNode(childNode, "SEARCH") != null)
                {
                    flags |= ColumnEncryptionFlags.Search;
                }
            }
            return flags;
        }


        /// <summary>
        /// Build column reference with column name.
        /// </summary>
        /// <param name="node">Parent node of column</param>
        /// <returns>Column reference</returns>
        private ColumnRef BuildColumnRef(ParseTreeNode node)
        {
            ColumnRef exp = null;

            // Without table name
            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("id_simple"))
            {
                exp = new ColumnRef(node.ChildNodes[0].Token.ValueString);
            }
            // With table name
            else if (node.ChildNodes.Count == 2 && node.ChildNodes[0].Term.Name.Equals("id_simple") && node.ChildNodes[1].Term.Name.Equals("id_simple"))
            {
                exp = new ColumnRef(node.ChildNodes[0].Token.ValueString, node.ChildNodes[1].Token.ValueString);
            }
            return exp;
        }


        /// <summary>
        /// Build ScalarFunction with parameters.
        /// </summary>
        /// <param name="node">Parent node of ScalarFunction</param>
        /// <returns>Function call</returns>
        private Expression BuildScalarFunction(ParseTreeNode node)
        {
            Expression exp = null;

            if (node.ChildNodes.Count == 1 && node.ChildNodes[0].Term.Name.Equals("exprList"))
            {
                exp = BuildExpression(node.ChildNodes[0]);
            }
            else if (node.ChildNodes.Count == 2 && node.ChildNodes[0].Term.Name.Equals("Id") && node.ChildNodes[1].Term.Name.Equals("funArgs"))
            {
                if (node.ChildNodes[0].ChildNodes.Count == 1 && node.ChildNodes[0].ChildNodes[0].Term.Name.Equals("id_simple"))
                {
                    exp = new ScalarFunction(node.ChildNodes[0].ChildNodes[0].Token.ValueString);
                }
                if (node.ChildNodes[1].ChildNodes.Count == 1 && node.ChildNodes[1].ChildNodes[0].Term.Name.Equals("exprList"))
                {
                    ((ScalarFunction)exp).Parameters = BuildExpressions(node.ChildNodes[1].ChildNodes[0]);
                }
            }
            return exp;
        }


        /// <summary>
        /// Build table reference with table name.
        /// </summary>
        /// <param name="node">Parent node of table</param>
        /// <returns>Table reference</returns>
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
