using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using PrismaDB.Commons;
using PrismaDB.QueryAST;
using PrismaDB.QueryParser.Postgres.AntlrGrammer;
using System;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.Postgres
{
    public static class PostgresQueryParser
    {
        public static List<Query> ParseToAst(String input)
        {
            try
            {
                var inputStream = new AntlrInputStream(input);
                var sqlLexer = new PostgresLexer(new CaseChangingCharStream(inputStream, true));
                var tokens = new CommonTokenStream(sqlLexer);
                var sqlParser = new PostgresParser(tokens);

                var visitor = new PostgresVisitor();
                var res = (List<Query>)visitor.Visit(sqlParser.root());
                return res;
            }
            catch (Exception ex) when (!(ex is PrismaParserException))
            {
                throw new PrismaParserException("Error occurred while parsing query.", ex);
            }
        }
    }

    public partial class PostgresVisitor : PostgresParserBaseVisitor<object>
    {
        public override object VisitRoot([NotNull] PostgresParser.RootContext context)
        {
            if (context.sqlStatements() == null)
                return new List<Query>();
            return Visit(context.sqlStatements());
        }

        public override object VisitSqlStatements([NotNull] PostgresParser.SqlStatementsContext context)
        {
            var queries = new List<Query>();
            foreach (var stmt in context.sqlStatement())
                queries.Add((Query)Visit(stmt));
            return queries;
        }
    }
}
