using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MySQL.AntlrGrammer;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.MySQL
{
    public partial class MySqlVisitor : MySqlParserBaseVisitor<object>
    {
        public override object VisitCreateTable([NotNull] MySqlParser.CreateTableContext context)
        {
            var res = new CreateTableQuery();
            res.TableName = (TableRef)Visit(context.tableName());
            res.ColumnDefinitions = (List<ColumnDefinition>)Visit(context.createDefinitions());
            return res;
        }

        public override object VisitCreateDefinitions([NotNull] MySqlParser.CreateDefinitionsContext context)
        {
            var res = new List<ColumnDefinition>();
            foreach (var createDefinition in context.createDefinition())
                res.Add((ColumnDefinition)Visit(createDefinition));
            return res;
        }

        public override object VisitColumnDeclaration([NotNull] MySqlParser.ColumnDeclarationContext context)
        {
            var res = (ColumnDefinition)Visit(context.columnDefinition());
            res.ColumnName = (Identifier)Visit(context.uid());
            return res;
        }

        public override object VisitColumnDefinition([NotNull] MySqlParser.ColumnDefinitionContext context)
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
            else if (context.AUTO_INCREMENT() != null)
                res.AutoIncrement = true;
            return res;
        }

        public override object VisitAlterTable([NotNull] MySqlParser.AlterTableContext context)
        {
            var res = new AlterTableQuery();
            res.AlterType = AlterType.MODIFY;
            res.TableName = (TableRef)Visit(context.tableName());
            res.AlteredColumns.Add((AlteredColumn)Visit(context.alterSpecification()));
            return res;
        }

        public override object VisitAlterByModifyColumn([NotNull] MySqlParser.AlterByModifyColumnContext context)
        {
            var colDef = (ColumnDefinition)Visit(context.columnDefinition());
            colDef.ColumnName = (Identifier)Visit(context.uid());
            var res = new AlteredColumn(colDef);
            return res;
        }

        public override object VisitDropTable([NotNull] MySqlParser.DropTableContext context)
        {
            var res = new DropTableQuery();
            var tables = (List<TableRef>)Visit(context.tables());
            if (tables.Count == 1)
                res.TableName = tables[0];
            return res;
        }
    }
}