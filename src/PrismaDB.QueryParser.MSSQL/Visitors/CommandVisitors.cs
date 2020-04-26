using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST.DCL;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MSSQL.AntlrGrammer;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.MSSQL
{
    public partial class MsSqlVisitor : MsSqlParserBaseVisitor<object>
    {
        public override object VisitKeysExportCommand([NotNull] MsSqlParser.KeysExportCommandContext context)
        {
            var res = new KeysExportCommand();
            if (context.stringLiteral() != null)
                res.FileUri = (StringConstant)Visit(context.stringLiteral());
            return res;
        }

        public override object VisitKeysUpdateCommand([NotNull] MsSqlParser.KeysUpdateCommandContext context)
        {
            var res = new KeysUpdateCommand();
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitEncryptCommand([NotNull] MsSqlParser.EncryptCommandContext context)
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

        public override object VisitDecryptCommand([NotNull] MsSqlParser.DecryptCommandContext context)
        {
            var res = new DecryptColumnCommand();
            res.Column = (ColumnRef)Visit(context.fullColumnName());
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitOpetreeRebalanceCommand([NotNull] MsSqlParser.OpetreeRebalanceCommandContext context)
        {
            var res = new OpetreeRebalanceCommand(false);
            //if (context.constants() != null)
            //    res.WithValues = (List<ConstantContainer>)Visit(context.constants());
            //if (context.STATUS() != null)
            //    res.StatusCheck = true;
            return res;
        }

        public override object VisitOpetreeSaveCommand([NotNull] MsSqlParser.OpetreeSaveCommandContext context)
        {
            return new OpetreeSaveCommand();
        }

        public override object VisitOpetreeLoadCommand([NotNull] MsSqlParser.OpetreeLoadCommandContext context)
        {
            return new OpetreeLoadCommand();
        }

        public override object VisitSchemaLoadCommand([NotNull] MsSqlParser.SchemaLoadCommandContext context)
        {
            return new SchemaLoadCommand();
        }

        public override object VisitSettingsSaveCommand([NotNull] MsSqlParser.SettingsSaveCommandContext context)
        {
            return new SettingsSaveCommand();
        }

        public override object VisitSettingsLoadCommand([NotNull] MsSqlParser.SettingsLoadCommandContext context)
        {
            return new SettingsLoadCommand();
        }

        public override object VisitBypassCommand([NotNull] MsSqlParser.BypassCommandContext context)
        {
            var res = new BypassCommand();

            if (context.ddlStatement() != null)
                res.Query = (DdlQuery)Visit(context.ddlStatement());

            if (context.dmlStatement() != null)
                res.Query = (DmlQuery)Visit(context.dmlStatement());

            return res;
        }

        public override object VisitLicenseRefreshCommand([NotNull] MsSqlParser.LicenseRefreshCommandContext context)
        {
            return new LicenseRefreshCommand();
        }

        public override object VisitLicenseSetKeyCommand([NotNull] MsSqlParser.LicenseSetKeyCommandContext context)
        {
            return new LicenseSetKeyCommand(((StringConstant)Visit(context.stringLiteral())).strvalue);
        }

        public override object VisitLicenseStatusCommand([NotNull] MsSqlParser.LicenseStatusCommandContext context)
        {
            return new LicenseStatusCommand();
        }
    }
}