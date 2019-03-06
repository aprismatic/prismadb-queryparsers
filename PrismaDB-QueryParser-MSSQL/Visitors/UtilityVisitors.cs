using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryParser.MSSQL.AntlrGrammer;

namespace PrismaDB.QueryParser.MSSQL
{
    public partial class MsSqlVisitor : MsSqlParserBaseVisitor<object>
    {
        public override object VisitUseStatement([NotNull] MsSqlParser.UseStatementContext context)
        {
            return new UseStatement((DatabaseRef)Visit(context.databaseName()));
        }

        public override object VisitShowTablesStatement([NotNull] MsSqlParser.ShowTablesStatementContext context)
        {
            return new ShowTablesQuery();
        }

        public override object VisitShowColumnsStatement([NotNull] MsSqlParser.ShowColumnsStatementContext context)
        {
            return new ShowColumnsQuery((TableRef)Visit(context.tableName()));
        }
    }
}