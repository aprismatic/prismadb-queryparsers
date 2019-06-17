using Antlr4.Runtime.Misc;
using PrismaDB.Commons;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MSSQL.AntlrGrammer;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.MSSQL
{
    public partial class MsSqlVisitor : MsSqlParserBaseVisitor<object>
    {
        public override object VisitCreateTable([NotNull] MsSqlParser.CreateTableContext context)
        {
            var res = new CreateTableQuery();
            res.TableName = (TableRef)Visit(context.tableName());
            res.ColumnDefinitions = (List<ColumnDefinition>)Visit(context.createDefinitions());
            return res;
        }

        public override object VisitCreateIndex([NotNull] MsSqlParser.CreateIndexContext context)
        {
            var res = new CreateIndexQuery();
            res.Name = (Identifier)Visit(context.indexName());
            res.OnTable = (TableRef)Visit(context.tableName());
            foreach (var col in context.fullColumnName())
                res.OnColumns.Add((ColumnRef)Visit(col));
            return res;
        }

        public override object VisitCreateDefinitions([NotNull] MsSqlParser.CreateDefinitionsContext context)
        {
            var res = new List<ColumnDefinition>();
            foreach (var createDefinition in context.createDefinition())
                res.Add((ColumnDefinition)Visit(createDefinition));
            return res;
        }

        public override object VisitColumnDeclaration([NotNull] MsSqlParser.ColumnDeclarationContext context)
        {
            var res = (ColumnDefinition)Visit(context.columnDefinition());
            res.ColumnName = (Identifier)Visit(context.uid());
            return res;
        }

        public override object VisitColumnDefinition([NotNull] MsSqlParser.ColumnDefinitionContext context)
        {
            var res = (ColumnDefinition)Visit(context.dataType());
            if (context.ENCRYPTED() != null)
            {
                res.EncryptionFlags = ColumnEncryptionFlags.Store;
                if (context.encryptionOptions() != null)
                    res.EncryptionFlags = (ColumnEncryptionFlags)Visit(context.encryptionOptions());
            }
            if (context.nullNotnull() != null)
                res.Nullable = (bool)Visit(context.nullNotnull());
            if (context.DEFAULT() != null)
                res.DefaultValue = (Expression)Visit(context.defaultValue());
            else if (context.IDENTITY() != null)
            {
                if (context.seed.GetText() != "1" || context.increment.GetText() != "1")
                    throw new PrismaParserException("IDENTITY currently only supports seed and increment value of 1.");
                res.AutoIncrement = true;
            }
            if (context.PRIMARY() != null)
                res.PrimaryKey = true;
            return res;
        }

        public override object VisitAlterTable([NotNull] MsSqlParser.AlterTableContext context)
        {
            var res = new AlterTableQuery();
            res.AlterType = AlterType.MODIFY;
            res.TableName = (TableRef)Visit(context.tableName());
            res.AlteredColumns.Add((AlteredColumn)Visit(context.alterSpecification()));
            return res;
        }

        public override object VisitAlterByModifyColumn([NotNull] MsSqlParser.AlterByModifyColumnContext context)
        {
            var colDef = (ColumnDefinition)Visit(context.columnDefinition());
            colDef.ColumnName = (Identifier)Visit(context.uid());
            var res = new AlteredColumn(colDef);
            return res;
        }

        public override object VisitDropTable([NotNull] MsSqlParser.DropTableContext context)
        {
            var res = new DropTableQuery();
            var tables = (List<TableRef>)Visit(context.tables());
            if (tables.Count == 1)
                res.TableName = tables[0];
            return res;
        }
    }
}