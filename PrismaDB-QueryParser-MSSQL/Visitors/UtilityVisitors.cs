using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryParser.MySQL.AntlrGrammer;

namespace PrismaDB.QueryParser.MySQL
{
    public partial class MySqlVisitor : MySqlParserBaseVisitor<object>
    {
        public override object VisitUseStatement([NotNull] MySqlParser.UseStatementContext context)
        {
            return new UseStatement((DatabaseRef)Visit(context.databaseName()));
        }

        public override object VisitShowTablesStatement([NotNull] MySqlParser.ShowTablesStatementContext context)
        {
            return new ShowTablesQuery();
        }

        public override object VisitShowColumnsStatement([NotNull] MySqlParser.ShowColumnsStatementContext context)
        {
            return new ShowColumnsQuery((TableRef)Visit(context.tableName()));
        }
    }
}