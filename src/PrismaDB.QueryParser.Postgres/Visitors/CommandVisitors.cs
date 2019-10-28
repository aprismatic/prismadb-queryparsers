using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST.DCL;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.Postgres.AntlrGrammer;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.Postgres
{
    public partial class PostgresVisitor : PostgresParserBaseVisitor<object>
    {
        public override object VisitExportKeysCommand([NotNull] PostgresParser.ExportKeysCommandContext context)
        {
            var res = new ExportKeysCommand();
            if (context.stringLiteral() != null)
                res.FileUri = (StringConstant)Visit(context.stringLiteral());
            return res;
        }

        public override object VisitUpdateKeysCommand([NotNull] PostgresParser.UpdateKeysCommandContext context)
        {
            var res = new UpdateKeysCommand();
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitEncryptCommand([NotNull] PostgresParser.EncryptCommandContext context)
        {
            var res = new EncryptColumnCommand();
            res.Column = (ColumnRef)Visit(context.fullColumnName());
            res.EncryptionFlags = ColumnEncryptionFlags.Store;
            if (context.encryptionOptions() != null)
                res.EncryptionFlags = (ColumnEncryptionFlags)Visit(context.encryptionOptions());
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitDecryptCommand([NotNull] PostgresParser.DecryptCommandContext context)
        {
            var res = new DecryptColumnCommand();
            res.Column = (ColumnRef)Visit(context.fullColumnName());
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitRebalanceOpetreeCommand([NotNull] PostgresParser.RebalanceOpetreeCommandContext context)
        {
            var res = new RebalanceOpetreeCommand();
            if (context.constants() != null)
                res.WithValues = (List<ConstantContainer>)Visit(context.constants());
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitSaveOpetreeCommand([NotNull] PostgresParser.SaveOpetreeCommandContext context)
        {
            return new SaveOpetreeCommand();
        }

        public override object VisitLoadOpetreeCommand([NotNull] PostgresParser.LoadOpetreeCommandContext context)
        {
            return new LoadOpetreeCommand();
        }

        public override object VisitLoadSchemaCommand([NotNull] PostgresParser.LoadSchemaCommandContext context)
        {
            return new LoadSchemaCommand();
        }

        public override object VisitSaveSettingsCommand([NotNull] PostgresParser.SaveSettingsCommandContext context)
        {
            return new SaveSettingsCommand();
        }

        public override object VisitLoadSettingsCommand([NotNull] PostgresParser.LoadSettingsCommandContext context)
        {
            return new LoadSettingsCommand();
        }

        public override object VisitBypassCommand([NotNull] PostgresParser.BypassCommandContext context)
        {
            var res = new BypassCommand();

            if (context.ddlStatement() != null)
                res.Query = (DdlQuery)Visit(context.ddlStatement());

            if (context.dmlStatement() != null)
                res.Query = (DmlQuery)Visit(context.dmlStatement());

            return res;
        }

        public override object VisitRefreshLicenseCommand([NotNull] PostgresParser.RefreshLicenseCommandContext context)
        {
            return new RefreshLicenseCommand();
        }

        public override object VisitSetLicenseKeyCommand([NotNull] PostgresParser.SetLicenseKeyCommandContext context)
        {
            return new SetLicenseKeyCommand(((StringConstant)Visit(context.stringLiteral())).strvalue);
        }

        public override object VisitCheckLicenseStatusCommand([NotNull] PostgresParser.CheckLicenseStatusCommandContext context)
        {
            return new CheckLicenseStatusCommand();
        }
    }
}