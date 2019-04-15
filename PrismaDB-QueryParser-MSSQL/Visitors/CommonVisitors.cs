using Antlr4.Runtime.Misc;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MSSQL.AntlrGrammer;
using System;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.MSSQL
{
    public partial class MsSqlVisitor : MsSqlParserBaseVisitor<object>
    {
        #region DB Objects
        public override object VisitDatabaseName([NotNull] MsSqlParser.DatabaseNameContext context)
        {
            return new DatabaseRef(((Identifier)Visit(context.uid())).id);
        }

        public override object VisitTableName([NotNull] MsSqlParser.TableNameContext context)
        {
            return new TableRef(((Identifier)Visit(context.uid())).id);
        }

        public override object VisitFullColumnName([NotNull] MsSqlParser.FullColumnNameContext context)
        {
            if (context.dottedId() == null)
                return new ColumnRef((Identifier)Visit(context.uid()));
            else
                return new ColumnRef(((Identifier)Visit(context.uid())).id, (Identifier)Visit(context.dottedId()));
        }

        public override object VisitUid([NotNull] MsSqlParser.UidContext context)
        {
            if (context.simpleId() != null)
                return Visit(context.simpleId());
            if (context.BRACKET_ID() != null)
                return new Identifier(context.BRACKET_ID().GetText().Trim('[', ']'));
            return null;
        }

        public override object VisitSimpleId([NotNull] MsSqlParser.SimpleIdContext context)
        {
            if (context.ID() != null)
                return new Identifier(context.ID().GetText());
            return null;
        }

        public override object VisitDottedId([NotNull] MsSqlParser.DottedIdContext context)
        {
            if (context.uid() != null)
                return Visit(context.uid());
            if (context.DOT_ID() != null)
                return new Identifier(context.DOT_ID().GetText().TrimStart('.'));
            return null;
        }
        #endregion


        #region Literals
        public override object VisitIntLiteral([NotNull] MsSqlParser.IntLiteralContext context)
        {
            return new IntConstant(Int64.Parse(context.INT_LITERAL().GetText()));
        }

        public override object VisitDecimalLiteral([NotNull] MsSqlParser.DecimalLiteralContext context)
        {
            return new DecimalConstant(Decimal.Parse(context.DECIMAL_LITERAL().GetText()));
        }

        public override object VisitStringLiteral([NotNull] MsSqlParser.StringLiteralContext context)
        {
            var str = context.STRING_LITERAL().GetText();
            if (str.StartsWith("'"))
            {
                str = str.Substring(1, str.Length - 2).Replace("\\'", "'").Replace("''", "'");
                return new StringConstant(str);
            }
            return null;
        }

        public override object VisitHexadecimalLiteral([NotNull] MsSqlParser.HexadecimalLiteralContext context)
        {
            var str = context.HEXADECIMAL_LITERAL().GetText().ToUpperInvariant();
            var length = 0;
            if (str.StartsWith("0X"))
                length = str.Length - 2;
            else
                length = str.Length - 3;
            var bytes = new byte[length / 2];
            for (var i = 0; i < bytes.Length; i++)
                bytes[i] = Convert.ToByte(str.Substring((i * 2) + 2, 2), 16);
            return new BinaryConstant(bytes);
        }

        public override object VisitNullNotnull([NotNull] MsSqlParser.NullNotnullContext context)
        {
            if (context.NOT() == null)
                return true;
            return false;
        }

        public override object VisitConstant([NotNull] MsSqlParser.ConstantContext context)
        {
            if (context.nullLiteral != null)
                return new NullConstant();
            else
                return base.VisitConstant(context);
        }
        #endregion


        #region Data Types
        public override object VisitStringDataType([NotNull] MsSqlParser.StringDataTypeContext context)
        {
            var res = new ColumnDefinition();
            switch (context.typeName.Text.ToUpperInvariant())
            {
                case "CHAR":
                    res.DataType = SqlDataType.MSSQL_CHAR;
                    break;
                case "VARCHAR":
                    res.DataType = SqlDataType.MSSQL_VARCHAR;
                    break;
                case "TEXT":
                    res.DataType = SqlDataType.MSSQL_TEXT;
                    break;
                case "NCHAR":
                    res.DataType = SqlDataType.MSSQL_NCHAR;
                    break;
                case "NVARCHAR":
                    res.DataType = SqlDataType.MSSQL_NVARCHAR;
                    break;
                case "NTEXT":
                    res.DataType = SqlDataType.MSSQL_NTEXT;
                    break;
            }
            if (context.lengthOneDimension() != null)
                res.Length = (int?)Visit(context.lengthOneDimension());
            return res;
        }

        public override object VisitSimpleDataType([NotNull] MsSqlParser.SimpleDataTypeContext context)
        {
            var res = new ColumnDefinition();
            switch (context.typeName.Text.ToUpperInvariant())
            {
                case "TINYINT":
                    res.DataType = SqlDataType.MSSQL_TINYINT;
                    break;
                case "SMALLINT":
                    res.DataType = SqlDataType.MSSQL_SMALLINT;
                    break;
                case "INT":
                    res.DataType = SqlDataType.MSSQL_INT;
                    break;
                case "BIGINT":
                    res.DataType = SqlDataType.MSSQL_BIGINT;
                    break;
                case "FLOAT":
                    res.DataType = SqlDataType.MSSQL_FLOAT;
                    break;
                case "DATE":
                    res.DataType = SqlDataType.MSSQL_DATE;
                    break;
                case "DATETIME":
                    res.DataType = SqlDataType.MSSQL_DATETIME;
                    break;
                case "UNIQUEIDENTIFIER":
                    res.DataType = SqlDataType.MSSQL_UNIQUEIDENTIFIER;
                    break;
            }
            return res;
        }

        public override object VisitDimensionDataType([NotNull] MsSqlParser.DimensionDataTypeContext context)
        {
            var res = new ColumnDefinition();
            switch (context.typeName.Text.ToUpperInvariant())
            {
                case "BINARY":
                    res.DataType = SqlDataType.MSSQL_BINARY;
                    break;
                case "VARBINARY":
                    res.DataType = SqlDataType.MSSQL_VARBINARY;
                    break;
            }
            if (context.lengthOneDimension() != null)
                res.Length = (int?)Visit(context.lengthOneDimension());
            return res;
        }

        public override object VisitLengthOneDimension([NotNull] MsSqlParser.LengthOneDimensionContext context)
        {
            if (context.intLiteral() != null)
                return (int?)((IntConstant)Visit(context.intLiteral())).intvalue;
            else if (context.MAX() != null)
                return -1;
            return null;
        }
        #endregion


        #region Common Lists
        public override object VisitUidList([NotNull] MsSqlParser.UidListContext context)
        {
            var res = new List<Identifier>();
            foreach (var uid in context.uid())
                res.Add((Identifier)Visit(uid));
            return res;
        }

        public override object VisitTables([NotNull] MsSqlParser.TablesContext context)
        {
            var res = new List<TableRef>();
            foreach (var tableName in context.tableName())
                res.Add((TableRef)Visit(tableName));
            return res;
        }

        public override object VisitExpressions([NotNull] MsSqlParser.ExpressionsContext context)
        {
            var res = new List<Expression>();
            foreach (var exp in context.expression())
                res.Add((Expression)Visit(exp));
            return res;
        }

        public override object VisitConstants([NotNull] MsSqlParser.ConstantsContext context)
        {
            var res = new List<Constant>();
            foreach (var constant in context.constant())
                res.Add((Constant)Visit(constant));
            return res;
        }
        #endregion


        #region Common Expressions
        public override object VisitCurrentTimestamp([NotNull] MsSqlParser.CurrentTimestampContext context)
        {
            return new ScalarFunction(context.GetText());
        }
        #endregion


        #region Functions
        public override object VisitFunctionCallExpressionAtom([NotNull] MsSqlParser.FunctionCallExpressionAtomContext context)
        {
            return Visit(context.functionCall());
        }

        public override object VisitScalarFunctionCall([NotNull] MsSqlParser.ScalarFunctionCallContext context)
        {
            ScalarFunction res;
            if (context.scalarFunctionName().SUM() != null)
                res = new SumAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().COUNT() != null)
                res = new CountAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().AVG() != null)
                res = new AvgAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().STDEV() != null)
                res = new StDevAggregationFunction(context.scalarFunctionName().GetText());
            else
                res = new ScalarFunction(context.scalarFunctionName().GetText());

            if (context.functionArgs() != null)
                foreach (var exp in (List<Expression>)Visit(context.functionArgs()))
                    res.AddChild(exp);

            return res;
        }

        public override object VisitUdfFunctionCall([NotNull] MsSqlParser.UdfFunctionCallContext context)
        {
            var res = new ScalarFunction((Identifier)Visit(context.uid()));
            if (context.functionArgs() != null)
                foreach (var exp in (List<Expression>)Visit(context.functionArgs()))
                    res.AddChild(exp);
            return res;
        }

        public override object VisitSimpleFunctionCall([NotNull] MsSqlParser.SimpleFunctionCallContext context)
        {
            return new ScalarFunction(context.GetText());
        }

        public override object VisitFunctionArgs([NotNull] MsSqlParser.FunctionArgsContext context)
        {
            var res = new List<Expression>();
            foreach (var arg in context.functionArg())
                if (arg.star == null)
                    res.Add((Expression)Visit(arg));
            return res;
        }
        #endregion


        #region Expressions, predicates
        public override object VisitNotExpression([NotNull] MsSqlParser.NotExpressionContext context)
        {
            var exp = Visit(context.expression());
            ((BooleanExpression)exp).NOT = !((BooleanExpression)exp).NOT;
            return exp;
        }

        public override object VisitLogicalExpression([NotNull] MsSqlParser.LogicalExpressionContext context)
        {
            switch (context.logicalOperator().GetText().ToUpperInvariant())
            {
                case "AND":
                    return new AndClause((Expression)Visit(context.expression()[0]), (Expression)Visit(context.expression()[1]));
                case "OR":
                    return new OrClause((Expression)Visit(context.expression()[0]), (Expression)Visit(context.expression()[1]));
                default:
                    return null;
            }
        }

        public override object VisitNestedExpression([NotNull] MsSqlParser.NestedExpressionContext context)
        {
            return Visit(context.expression());
        }

        public override object VisitInPredicate([NotNull] MsSqlParser.InPredicateContext context)
        {
            var res = new BooleanIn();
            res.Column = (ColumnRef)Visit(context.predicate());
            foreach (var exp in (List<Expression>)Visit(context.expressions()))
                res.AddChild((Constant)exp);
            if (context.NOT() != null)
                res.NOT = true;
            return res;
        }

        public override object VisitIsNullPredicate([NotNull] MsSqlParser.IsNullPredicateContext context)
        {
            return new BooleanIsNull((ColumnRef)Visit(context.predicate()), !(bool)Visit(context.nullNotnull()));
        }

        public override object VisitBinaryComparasionPredicate([NotNull] MsSqlParser.BinaryComparasionPredicateContext context)
        {
            switch (context.comparisonOperator().GetText())
            {
                case "=":
                    return new BooleanEquals((Expression)Visit(context.left), (Expression)Visit(context.right));
                case ">":
                    return new BooleanGreaterThan((Expression)Visit(context.left), (Expression)Visit(context.right));
                case "<":
                    return new BooleanGreaterThan((Expression)Visit(context.right), (Expression)Visit(context.left));
                case ">=":
                    {
                        var exprLeft = new BooleanGreaterThan((Expression)Visit(context.left), (Expression)Visit(context.right));
                        var exprRight = new BooleanEquals((Expression)Visit(context.left), (Expression)Visit(context.right));
                        return new OrClause(exprLeft, exprRight);
                    }
                case "<=":
                    {
                        var exprLeft = new BooleanGreaterThan((Expression)Visit(context.right), (Expression)Visit(context.left));
                        var exprRight = new BooleanEquals((Expression)Visit(context.right), (Expression)Visit(context.left));
                        return new OrClause(exprLeft, exprRight);
                    }
                case "<>":
                case "!=":
                    return new BooleanEquals((Expression)Visit(context.left), (Expression)Visit(context.right), true);
                default:
                    return null;
            }
        }

        public override object VisitLikePredicate([NotNull] MsSqlParser.LikePredicateContext context)
        {
            var res = new BooleanLike();
            res.Column = (ColumnRef)Visit(context.predicate()[0]);
            res.SearchValue = (StringConstant)Visit(context.predicate()[1]);
            if (context.NOT() != null)
                res.NOT = true;
            if (context.stringLiteral() != null)
                res.EscapeChar = ((StringConstant)Visit(context.stringLiteral())).strvalue[0];
            return res;
        }

        public override object VisitMathExpressionPredicate([NotNull] MsSqlParser.MathExpressionPredicateContext context)
        {
            return Visit(context.addSubExpression());
        }

        public override object VisitNestedPredicate([NotNull] MsSqlParser.NestedPredicateContext context)
        {
            return Visit(context.predicate());
        }

        public override object VisitNestedMulDivExpression([NotNull] MsSqlParser.NestedMulDivExpressionContext context)
        {
            return Visit(context.mulDivExpression());
        }

        public override object VisitNestedAddSubExpression([NotNull] MsSqlParser.NestedAddSubExpressionContext context)
        {
            return Visit(context.addSubExpression());
        }

        public override object VisitSimpleExpressionAtom([NotNull] MsSqlParser.SimpleExpressionAtomContext context)
        {
            return Visit(context.expressionAtom());
        }

        public override object VisitNestedAddSubExpressionInMulDiv([NotNull] MsSqlParser.NestedAddSubExpressionInMulDivContext context)
        {
            return Visit(context.addSubExpression());
        }

        public override object VisitAddSubExpressionAtom([NotNull] MsSqlParser.AddSubExpressionAtomContext context)
        {
            switch (context.addSubOperator().GetText())
            {
                case "+":
                    return new Addition((Expression)Visit(context.left), (Expression)Visit(context.right));
                case "-":
                    return new Subtraction((Expression)Visit(context.left), (Expression)Visit(context.right));
                default:
                    return null;
            }
        }

        public override object VisitMulDivExpressionAtom([NotNull] MsSqlParser.MulDivExpressionAtomContext context)
        {
            switch (context.mulDivOperator().GetText())
            {
                case "*":
                    return new Multiplication((Expression)Visit(context.left), (Expression)Visit(context.right));
                case "/":
                    return new Division((Expression)Visit(context.left), (Expression)Visit(context.right));
                default:
                    return null;
            }
        }

        public override object VisitUnaryExpressionAtom([NotNull] MsSqlParser.UnaryExpressionAtomContext context)
        {
            var exp = Visit(context.expressionAtom());
            ((BooleanExpression)exp).NOT = !((BooleanExpression)exp).NOT;
            return exp;
        }

        public override object VisitNestedExpressionAtom([NotNull] MsSqlParser.NestedExpressionAtomContext context)
        {
            return Visit(context.expressionAtom());
        }
        #endregion


        #region Encryption
        public override object VisitEncryptionOptions([NotNull] MsSqlParser.EncryptionOptionsContext context)
        {
            var res = ColumnEncryptionFlags.None;
            foreach (var encType in context.encryptionType())
                res |= (ColumnEncryptionFlags)Visit(encType);
            return res;
        }

        public override object VisitEncryptionType([NotNull] MsSqlParser.EncryptionTypeContext context)
        {
            switch (context.GetText().ToUpperInvariant())
            {
                case "ADDITION":
                    return ColumnEncryptionFlags.Addition;
                case "SEARCH":
                    return ColumnEncryptionFlags.Search;
                case "STORE":
                    return ColumnEncryptionFlags.Store;
                case "MULTIPLICATION":
                    return ColumnEncryptionFlags.Multiplication;
                case "RANGE":
                    return ColumnEncryptionFlags.Range;
                case "WILDCARD":
                    return ColumnEncryptionFlags.Wildcard;
            }
            return null;
        }
        #endregion
    }
}