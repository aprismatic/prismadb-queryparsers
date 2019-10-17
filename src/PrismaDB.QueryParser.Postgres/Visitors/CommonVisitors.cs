using Antlr4.Runtime.Misc;
using PrismaDB.Commons;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.Postgres.AntlrGrammer;
using System;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.Postgres
{
    public partial class PostgresVisitor : PostgresParserBaseVisitor<object>
    {
        #region DB Objects
        public override object VisitDatabaseName([NotNull] PostgresParser.DatabaseNameContext context)
        {
            return new DatabaseRef(((Identifier)Visit(context.uid())).id);
        }

        public override object VisitTableName([NotNull] PostgresParser.TableNameContext context)
        {
            return new TableRef(((Identifier)Visit(context.uid())).id);
        }

        public override object VisitFullColumnName([NotNull] PostgresParser.FullColumnNameContext context)
        {
            if (context.dottedId() == null)
                return new ColumnRef((Identifier)Visit(context.uid()));
            else
                return new ColumnRef(((Identifier)Visit(context.uid())).id, (Identifier)Visit(context.dottedId()));
        }

        public override object VisitUid([NotNull] PostgresParser.UidContext context)
        {
            if (context.simpleId() != null)
                return Visit(context.simpleId());
            if (context.DOUBLE_QUOTE_ID() != null)
                return new Identifier(context.DOUBLE_QUOTE_ID().GetText().Trim('"'));
            return null;
        }

        public override object VisitSimpleId([NotNull] PostgresParser.SimpleIdContext context)
        {
            if (context.ID() != null)
                return new Identifier(context.ID().GetText());
            return null;
        }

        public override object VisitDottedId([NotNull] PostgresParser.DottedIdContext context)
        {
            if (context.uid() != null)
                return Visit(context.uid());
            if (context.DOT_ID() != null)
                return new Identifier(context.DOT_ID().GetText().TrimStart('.'));
            return null;
        }
        #endregion


        #region Literals
        public override object VisitIntLiteral([NotNull] PostgresParser.IntLiteralContext context)
        {
            return new IntConstant(Int64.Parse(context.INT_LITERAL().GetText()));
        }

        public override object VisitDecimalLiteral([NotNull] PostgresParser.DecimalLiteralContext context)
        {
            return new DecimalConstant(Decimal.Parse(context.DECIMAL_LITERAL().GetText()));
        }

        public override object VisitStringLiteral([NotNull] PostgresParser.StringLiteralContext context)
        {
            var str = context.STRING_LITERAL().GetText();
            if (str.StartsWith("'"))
            {
                str = str.Substring(1, str.Length - 2).Replace("''", "'");
                return new StringConstant(str);
            }
            return null;
        }

        public override object VisitHexadecimalLiteral([NotNull] PostgresParser.HexadecimalLiteralContext context)
        {
            if (context.HEXADECIMAL_LITERAL().GetText()[4] == 'X')
                throw new PrismaParserException("Error parsing hexadecimal literal.");
            var str = context.HEXADECIMAL_LITERAL().GetText().ToUpperInvariant();
            var length = 0;
            length = str.Length - 6;
            var bytes = new byte[length / 2];
            for (var i = 0; i < bytes.Length; i++)
                bytes[i] = Convert.ToByte(str.Substring((i * 2) + 5, 2), 16);
            return new BinaryConstant(bytes);
        }

        public override object VisitParameter([NotNull] PostgresParser.ParameterContext context)
        {
            return new PlaceholderConstant(label: context.PARAMETER().GetText());
        }

        public override object VisitNullNotnull([NotNull] PostgresParser.NullNotnullContext context)
        {
            if (context.NOT() == null)
                return true;
            return false;
        }

        public override object VisitConstant([NotNull] PostgresParser.ConstantContext context)
        {
            if (context.nullLiteral() != null)
                return new ConstantContainer(new NullConstant());
            else
                return new ConstantContainer(base.VisitConstant(context));
        }
        #endregion


        #region Data Types
        public override object VisitStringDataType([NotNull] PostgresParser.StringDataTypeContext context)
        {
            var res = new ColumnDefinition();
            switch (context.typeName.Text.ToUpperInvariant())
            {
                case "CHAR":
                    res.DataType = SqlDataType.Postgres_CHAR;
                    break;
                case "VARCHAR":
                    res.DataType = SqlDataType.Postgres_VARCHAR;
                    break;
                case "TEXT":
                    res.DataType = SqlDataType.Postgres_TEXT;
                    break;
            }
            if (context.lengthOneDimension() != null)
                res.Length = (int?)Visit(context.lengthOneDimension());
            return res;
        }

        public override object VisitSimpleDataType([NotNull] PostgresParser.SimpleDataTypeContext context)
        {
            var res = new ColumnDefinition();
            switch (context.typeName.Text.ToUpperInvariant())
            {
                case "INT2":
                case "SMALLINT":
                    res.DataType = SqlDataType.Postgres_INT2;
                    break;
                case "INT4":
                case "INT":
                case "INTEGER":
                    res.DataType = SqlDataType.Postgres_INT4;
                    break;
                case "INT8":
                case "BIGINT":
                    res.DataType = SqlDataType.Postgres_INT8;
                    break;
                case "SERIAL":
                    res.DataType = SqlDataType.Postgres_INT4;
                    res.AutoIncrement = true;
                    res.Nullable = false;
                    break;
                case "FLOAT4":
                case "REAL":
                    res.DataType = SqlDataType.Postgres_FLOAT4;
                    break;
                case "FLOAT8":
                    res.DataType = SqlDataType.Postgres_FLOAT8;
                    break;
                case "BYTEA":
                    res.DataType = SqlDataType.Postgres_BYTEA;
                    break;
                case "DATE":
                    res.DataType = SqlDataType.Postgres_DATE;
                    break;
                case "TIMESTAMP":
                    res.DataType = SqlDataType.Postgres_TIMESTAMP;
                    break;
                case "DECIMAL":
                    res.DataType = SqlDataType.Postgres_DECIMAL;
                    break;
                default:
                    if (context.typeName.Text.ToUpperInvariant().StartsWith("DOUBLE"))
                        res.DataType = SqlDataType.Postgres_FLOAT8;
                    break;
            }
            return res;
        }

        public override object VisitLengthOneDimension([NotNull] PostgresParser.LengthOneDimensionContext context)
        {
            if (context.intLiteral() != null)
                return (int?)((IntConstant)Visit(context.intLiteral())).intvalue;
            return null;
        }
        #endregion


        #region Common Lists
        public override object VisitUidList([NotNull] PostgresParser.UidListContext context)
        {
            var res = new List<Identifier>();
            foreach (var uid in context.uid())
                res.Add((Identifier)Visit(uid));
            return res;
        }

        public override object VisitTables([NotNull] PostgresParser.TablesContext context)
        {
            var res = new List<TableRef>();
            foreach (var tableName in context.tableName())
                res.Add((TableRef)Visit(tableName));
            return res;
        }

        public override object VisitExpressions([NotNull] PostgresParser.ExpressionsContext context)
        {
            var res = new List<Expression>();
            foreach (var exp in context.expression())
                res.Add((Expression)Visit(exp));
            return res;
        }

        public override object VisitConstants([NotNull] PostgresParser.ConstantsContext context)
        {
            var res = new List<ConstantContainer>();
            foreach (var constant in context.constant())
                res.Add((ConstantContainer)Visit(constant));
            return res;
        }
        #endregion


        #region Common Expressions
        public override object VisitCurrentTimestamp([NotNull] PostgresParser.CurrentTimestampContext context)
        {
            return new ScalarFunction(context.GetText());
        }
        #endregion


        #region Functions
        public override object VisitFunctionCallExpressionAtom([NotNull] PostgresParser.FunctionCallExpressionAtomContext context)
        {
            return Visit(context.functionCall());
        }

        public override object VisitScalarFunctionCall([NotNull] PostgresParser.ScalarFunctionCallContext context)
        {
            ScalarFunction res;
            if (context.scalarFunctionName().SUM() != null)
                res = new SumAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().COUNT() != null)
                res = new CountAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().AVG() != null)
                res = new AvgAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().MIN() != null)
                res = new MinAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().MAX() != null)
                res = new MaxAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().STDDEV_SAMP() != null)
                res = new StDevAggregationFunction(context.scalarFunctionName().GetText());
            else if (context.scalarFunctionName().LINREG() != null)
                res = new LinRegAggregationFunction(context.scalarFunctionName().GetText());
            else
                res = new ScalarFunction(context.scalarFunctionName().GetText());

            if (context.functionArgs() != null)
                foreach (var exp in (List<Expression>)Visit(context.functionArgs()))
                    res.AddChild(exp);

            return res;
        }

        public override object VisitUdfFunctionCall([NotNull] PostgresParser.UdfFunctionCallContext context)
        {
            var res = new ScalarFunction((Identifier)Visit(context.uid()));
            if (context.functionArgs() != null)
                foreach (var exp in (List<Expression>)Visit(context.functionArgs()))
                    res.AddChild(exp);
            return res;
        }

        public override object VisitSimpleFunctionCall([NotNull] PostgresParser.SimpleFunctionCallContext context)
        {
            return new ScalarFunction(context.GetText());
        }

        public override object VisitFunctionArgs([NotNull] PostgresParser.FunctionArgsContext context)
        {
            var res = new List<Expression>();
            foreach (var arg in context.functionArg())
                if (arg.star == null)
                    res.Add((Expression)Visit(arg));
            return res;
        }
        #endregion


        #region Expressions, predicates
        public override object VisitNotExpression([NotNull] PostgresParser.NotExpressionContext context)
        {
            var exp = Visit(context.expression());
            ((BooleanExpression)exp).NOT = !((BooleanExpression)exp).NOT;
            return exp;
        }

        public override object VisitLogicalExpression([NotNull] PostgresParser.LogicalExpressionContext context)
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

        public override object VisitNestedExpression([NotNull] PostgresParser.NestedExpressionContext context)
        {
            return Visit(context.expression());
        }

        public override object VisitInPredicate([NotNull] PostgresParser.InPredicateContext context)
        {
            var res = new BooleanIn();
            res.Column = (ColumnRef)Visit(context.predicate());
            foreach (var exp in (List<Expression>)Visit(context.expressions()))
                res.AddChild((ConstantContainer)exp);
            if (context.NOT() != null)
                res.NOT = true;
            return res;
        }

        public override object VisitIsNullPredicate([NotNull] PostgresParser.IsNullPredicateContext context)
        {
            return new BooleanIsNull((ColumnRef)Visit(context.predicate()), !(bool)Visit(context.nullNotnull()));
        }

        public override object VisitBinaryComparasionPredicate([NotNull] PostgresParser.BinaryComparasionPredicateContext context)
        {
            switch (context.comparisonOperator().GetText())
            {
                case "=":
                    return new BooleanEquals((Expression)Visit(context.left), (Expression)Visit(context.right));
                case ">":
                    return new BooleanGreaterThan((Expression)Visit(context.left), (Expression)Visit(context.right));
                case "<":
                    return new BooleanLessThan((Expression)Visit(context.left), (Expression)Visit(context.right));
                case ">=":
                    return new BooleanGreaterThanEquals((Expression)Visit(context.left), (Expression)Visit(context.right));
                case "<=":
                    return new BooleanLessThanEquals((Expression)Visit(context.left), (Expression)Visit(context.right));
                case "<>":
                case "!=":
                    return new BooleanEquals((Expression)Visit(context.left), (Expression)Visit(context.right), true);
                default:
                    return null;
            }
        }

        public override object VisitLikePredicate([NotNull] PostgresParser.LikePredicateContext context)
        {
            var res = new BooleanLike();
            res.Column = (ColumnRef)Visit(context.predicate()[0]);
            res.SearchValue = (ConstantContainer)Visit(context.predicate()[1]);
            if (context.NOT() != null)
                res.NOT = true;
            if (context.stringLiteral() != null)
                res.EscapeChar = ((StringConstant)Visit(context.stringLiteral())).strvalue[0];
            return res;
        }

        public override object VisitMathExpressionPredicate([NotNull] PostgresParser.MathExpressionPredicateContext context)
        {
            return Visit(context.addSubExpression());
        }

        public override object VisitNestedPredicate([NotNull] PostgresParser.NestedPredicateContext context)
        {
            return Visit(context.predicate());
        }

        public override object VisitNestedMulDivExpression([NotNull] PostgresParser.NestedMulDivExpressionContext context)
        {
            return Visit(context.mulDivExpression());
        }

        public override object VisitNestedAddSubExpression([NotNull] PostgresParser.NestedAddSubExpressionContext context)
        {
            return Visit(context.addSubExpression());
        }

        public override object VisitSimpleExpressionAtom([NotNull] PostgresParser.SimpleExpressionAtomContext context)
        {
            return Visit(context.expressionAtom());
        }

        public override object VisitNestedAddSubExpressionInMulDiv([NotNull] PostgresParser.NestedAddSubExpressionInMulDivContext context)
        {
            return Visit(context.addSubExpression());
        }

        public override object VisitAddSubExpressionAtom([NotNull] PostgresParser.AddSubExpressionAtomContext context)
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

        public override object VisitMulDivExpressionAtom([NotNull] PostgresParser.MulDivExpressionAtomContext context)
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

        public override object VisitUnaryExpressionAtom([NotNull] PostgresParser.UnaryExpressionAtomContext context)
        {
            var exp = Visit(context.expressionAtom());
            ((BooleanExpression)exp).NOT = !((BooleanExpression)exp).NOT;
            return exp;
        }

        public override object VisitNestedExpressionAtom([NotNull] PostgresParser.NestedExpressionAtomContext context)
        {
            return Visit(context.expressionAtom());
        }
        #endregion


        #region Encryption
        public override object VisitEncryptionOptions([NotNull] PostgresParser.EncryptionOptionsContext context)
        {
            var res = ColumnEncryptionFlags.None;
            foreach (var encType in context.encryptionType())
                res |= (ColumnEncryptionFlags)Visit(encType);
            return res;
        }

        public override object VisitEncryptionType([NotNull] PostgresParser.EncryptionTypeContext context)
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