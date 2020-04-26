using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST.DCL;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MySQL.AntlrGrammer;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.MySQL
{
    public partial class MySqlVisitor : MySqlParserBaseVisitor<object>
    {
        public override object VisitKeysExportCommand([NotNull] MySqlParser.KeysExportCommandContext context)
        {
            var res = new KeysExportCommand();
            if (context.stringLiteral() != null)
                res.FileUri = (StringConstant)Visit(context.stringLiteral());
            return res;
        }

        public override object VisitKeysUpdateCommand([NotNull] MySqlParser.KeysUpdateCommandContext context)
        {
            var res = new KeysUpdateCommand();
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitEncryptCommand([NotNull] MySqlParser.EncryptCommandContext context)
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

        public override object VisitDecryptCommand([NotNull] MySqlParser.DecryptCommandContext context)
        {
            var res = new DecryptColumnCommand();
            res.Column = (ColumnRef)Visit(context.fullColumnName());
            if (context.STATUS() != null)
                res.StatusCheck = true;
            return res;
        }

        public override object VisitRegisterUserCommand([NotNull] MySqlParser.RegisterUserCommandContext context)
        {
            var res = new RegisterUserCommand();
            res.UserId = (StringConstant)Visit(context.user);
            res.Password = (StringConstant)Visit(context.password);
            return res;
        }

        public override object VisitOpetreeRebalanceCommand([NotNull] MySqlParser.OpetreeRebalanceCommandContext context)
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

            res.StopAfter = (DecimalConstant)Visit(context.stopAfter);

            if (context.ITERATIONS() != null)
                res.StopType = RebalanceStopType.ITERATIONS;
            if (context.HOURS() != null)
                res.StopType = RebalanceStopType.HOURS;
            if (context.MINUTES() != null)
                res.StopType = RebalanceStopType.MINUTES;

            return res;
        }

        public override object VisitOpetreeSaveCommand([NotNull] MySqlParser.OpetreeSaveCommandContext context)
        {
            return new OpetreeSaveCommand();
        }

        public override object VisitOpetreeLoadCommand([NotNull] MySqlParser.OpetreeLoadCommandContext context)
        {
            return new OpetreeLoadCommand();
        }

        public override object VisitOpetreeRebuildCommand([NotNull] MySqlParser.OpetreeRebuildCommandContext context)
        {
            return new OpetreeRebuildCommand(context.STATUS() != null);
        }

        public override object VisitOpetreeInsertCommand([NotNull] MySqlParser.OpetreeInsertCommandContext context)
        {
            var res = new OpetreeInsertCommand();
            if (context.constants() != null)
                res.Values = (List<ConstantContainer>)Visit(context.constants());
            return res;
        }

        public override object VisitOpetreeStatusCommand([NotNull] MySqlParser.OpetreeStatusCommandContext context)
        {
            return new OpetreeStatusCommand();
        }

        public override object VisitSchemaLoadCommand([NotNull] MySqlParser.SchemaLoadCommandContext context)
        {
            return new SchemaLoadCommand();
        }

        public override object VisitSettingsSaveCommand([NotNull] MySqlParser.SettingsSaveCommandContext context)
        {
            return new SettingsSaveCommand();
        }

        public override object VisitSettingsLoadCommand([NotNull] MySqlParser.SettingsLoadCommandContext context)
        {
            return new SettingsLoadCommand();
        }

        public override object VisitBypassCommand([NotNull] MySqlParser.BypassCommandContext context)
        {
            var res = new BypassCommand();

            if (context.ddlStatement() != null)
                res.Query = (DdlQuery)Visit(context.ddlStatement());

            if (context.dmlStatement() != null)
                res.Query = (DmlQuery)Visit(context.dmlStatement());

            return res;
        }

        public override object VisitLicenseRefreshCommand([NotNull] MySqlParser.LicenseRefreshCommandContext context)
        {
            return new LicenseRefreshCommand();
        }

        public override object VisitLicenseSetKeyCommand([NotNull] MySqlParser.LicenseSetKeyCommandContext context)
        {
            return new LicenseSetKeyCommand(((StringConstant)Visit(context.stringLiteral())).strvalue);
        }

        public override object VisitLicenseStatusCommand([NotNull] MySqlParser.LicenseStatusCommandContext context)
        {
            return new LicenseStatusCommand();
        }
    }
}