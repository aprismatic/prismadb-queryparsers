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
        public override object VisitKeysExportCommand([NotNull] PostgresParser.KeysExportCommandContext context)
        {
            var res = new KeysExportCommand();
            if (context.stringLiteral() != null)
                res.FileUri = (StringConstant)Visit(context.stringLiteral());
            return res;
        }

        public override object VisitKeysUpdateCommand([NotNull] PostgresParser.KeysUpdateCommandContext context)
        {
            var res = new KeysUpdateCommand();
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

        public override object VisitOpetreeRebalanceCommand([NotNull] PostgresParser.OpetreeRebalanceCommandContext context)
        {
            var res = new OpetreeRebalanceCommand();
            if (context.STATUS() != null)
                res.StatusCheck = true;

            if (context.STOP() == null)
            {
                res.StopType = RebalanceStopType.FULL;
                return res;
            }

            if (context.AFTER() == null)
            {
                res.StopType = RebalanceStopType.IMMEDIATE;
                return res;
            }

            res.StopAfter = (IntConstant)Visit(context.stopAfter);

            if (context.ITERATIONS() != null)
                res.StopType = RebalanceStopType.ITERATIONS;
            if (context.HOURS() != null)
                res.StopType = RebalanceStopType.HOURS;
            if (context.MINUTES() != null)
                res.StopType = RebalanceStopType.MINUTES;

            return res;
        }

        public override object VisitOpetreeSaveCommand([NotNull] PostgresParser.OpetreeSaveCommandContext context)
        {
            return new OpetreeSaveCommand();
        }

        public override object VisitOpetreeLoadCommand([NotNull] PostgresParser.OpetreeLoadCommandContext context)
        {
            return new OpetreeLoadCommand();
        }

        public override object VisitOpetreeRebuildCommand([NotNull] PostgresParser.OpetreeRebuildCommandContext context)
        {
            return new OpetreeRebuildCommand(context.STATUS() != null);
        }

        public override object VisitOpetreeInsertCommand([NotNull] PostgresParser.OpetreeInsertCommandContext context)
        {
            var res = new OpetreeInsertCommand();
            if (context.constants() != null)
                res.Values = (List<ConstantContainer>)Visit(context.constants());
            return res;
        }

        public override object VisitOpetreeStatusCommand([NotNull] PostgresParser.OpetreeStatusCommandContext context)
        {
            return new OpetreeStatusCommand();
        }

        public override object VisitSchemaLoadCommand([NotNull] PostgresParser.SchemaLoadCommandContext context)
        {
            return new SchemaLoadCommand();
        }

        public override object VisitSettingsSaveCommand([NotNull] PostgresParser.SettingsSaveCommandContext context)
        {
            return new SettingsSaveCommand();
        }

        public override object VisitSettingsLoadCommand([NotNull] PostgresParser.SettingsLoadCommandContext context)
        {
            return new SettingsLoadCommand();
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

        public override object VisitLicenseRefreshCommand([NotNull] PostgresParser.LicenseRefreshCommandContext context)
        {
            return new LicenseRefreshCommand();
        }

        public override object VisitLicenseSetKeyCommand([NotNull] PostgresParser.LicenseSetKeyCommandContext context)
        {
            return new LicenseSetKeyCommand(((StringConstant)Visit(context.stringLiteral())).strvalue);
        }

        public override object VisitLicenseStatusCommand([NotNull] PostgresParser.LicenseStatusCommandContext context)
        {
            return new LicenseStatusCommand();
        }
    }
}