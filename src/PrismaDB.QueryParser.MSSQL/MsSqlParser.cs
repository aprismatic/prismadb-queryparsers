using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using PrismaDB.Commons;
using PrismaDB.QueryAST;
using PrismaDB.QueryParser.MSSQL.AntlrGrammer;
using System;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.MSSQL
{
    public static class MsSqlQueryParser
    {
        public static List<Query> ParseToAst(String input)
        {
            try
            {
                var inputStream = new AntlrInputStream(input);
                var sqlLexer = new MsSqlLexer(new CaseChangingCharStream(inputStream, true));
                var tokens = new CommonTokenStream(sqlLexer);
                var sqlParser = new MsSqlParser(tokens);

                var visitor = new MsSqlVisitor();
                var res = (List<Query>)visitor.Visit(sqlParser.root());
                return res;
            }
            catch (Exception ex) when (!(ex is PrismaParserException))
            {
                throw new PrismaParserException("Error occurred while parsing query.", ex);
            }
        }
    }

    public partial class MsSqlVisitor : MsSqlParserBaseVisitor<object>
    {
        public override object VisitRoot([NotNull] MsSqlParser.RootContext context)
        {
            if (context.sqlStatements() == null)
                return new List<Query>();
            return Visit(context.sqlStatements());
        }

        public override object VisitSqlStatements([NotNull] MsSqlParser.SqlStatementsContext context)
        {
            var queries = new List<Query>();
            foreach (var stmt in context.sqlStatement())
                queries.Add((Query)Visit(stmt));
            return queries;
        }
    }
}
