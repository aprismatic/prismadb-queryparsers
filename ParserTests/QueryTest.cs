using System;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DCL;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MSSQL;
using Xunit;

namespace ParserTests
{
    public class QueryTest
    {
        [Fact(DisplayName = "Parse ALTER TABLE")]
        public void Parse_AlterTable()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "ALTER TABLE table1 " +
                       "ALTER COLUMN col1 TEXT ENCRYPTED FOR (STORE, SEARCH) NULL";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (AlterTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);
            Assert.Equal(AlterType.MODIFY, actual.AlterType);

            Assert.Equal(new Identifier("col1"), actual.AlteredColumns[0].ColumnDefinition.ColumnName);
            Assert.Equal(SqlDataType.MSSQL_TEXT, actual.AlteredColumns[0].ColumnDefinition.DataType);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Store & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Search & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.IntegerAddition & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.IntegerMultiplication &
                actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.True(actual.AlteredColumns[0].ColumnDefinition.Nullable);
            Assert.Null(actual.AlteredColumns[0].ColumnDefinition.Length);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\DATETIME")]
        public void Parse_CreateTable_DATETIME()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "CREATE TABLE table1 " +
                       "(col1 DATETIME NOT NULL)";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);

            Assert.Equal(new Identifier("col1"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_DATETIME, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.False(actual.ColumnDefinitions[0].Nullable);
            Assert.Null(actual.ColumnDefinitions[0].Length);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\TEXT")]
        public void Parse_CreateTable_TEXT()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "CREATE TABLE table1 " +
                       "(col1 TEXT, " +
                       "col2 TEXT ENCRYPTED FOR (STORE, SEARCH) NULL)";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);

            Assert.Equal(new Identifier("col1"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_TEXT, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[0].Nullable);
            Assert.Null(actual.ColumnDefinitions[0].Length);

            Assert.Equal(new Identifier("col2"), actual.ColumnDefinitions[1].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_TEXT, actual.ColumnDefinitions[1].DataType);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Store & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Search & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.IntegerAddition & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.IntegerMultiplication & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[1].Nullable);
            Assert.Null(actual.ColumnDefinitions[1].Length);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\partial encryption")]
        public void Parse_CreateTable_WithPartialEncryption()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "CREATE TABLE ttt " +
                       "(aaa INT ENCRYPTED FOR (INTEGER_ADDITION, INTEGER_MULTIPLICATION) NOT NULL IDENTITY(1,1), " +
                       "[bbb] BIGINT NULL, " +
                       "ccc VARCHAR(80) NOT NULL, " +
                       "ddd VARCHAR(MAX) ENCRYPTED FOR (STORE, SEARCH), " +
                       "eee TEXT NULL, " +
                       "fff TEXT ENCRYPTED NULL, " +
                       "ggg FLOAT," +
                       "hhh DATETIME ENCRYPTED DEFAULT CURRENT_TIMESTAMP" + ")";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("ttt"), actual.TableName);
            Assert.Equal(new Identifier("aaa"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_INT, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.IntegerAddition | ColumnEncryptionFlags.IntegerMultiplication,
                actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[0].AutoIncrement);
            Assert.False(actual.ColumnDefinitions[0].Nullable);
            Assert.Equal(new Identifier("bbb"), actual.ColumnDefinitions[1].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_BIGINT, actual.ColumnDefinitions[1].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[1].Nullable);
            Assert.False(actual.ColumnDefinitions[1].AutoIncrement);
            Assert.Equal(new Identifier("ccc"), actual.ColumnDefinitions[2].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_VARCHAR, actual.ColumnDefinitions[2].DataType);
            Assert.Equal(80, actual.ColumnDefinitions[2].Length);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[2].EncryptionFlags);
            Assert.False(actual.ColumnDefinitions[2].Nullable);
            Assert.Equal(new Identifier("ddd"), actual.ColumnDefinitions[3].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_VARCHAR, actual.ColumnDefinitions[3].DataType);
            Assert.Equal(-1, actual.ColumnDefinitions[3].Length);
            Assert.Equal(ColumnEncryptionFlags.Store | ColumnEncryptionFlags.Search,
                actual.ColumnDefinitions[3].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[3].Nullable);
            Assert.Equal(new Identifier("eee"), actual.ColumnDefinitions[4].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_TEXT, actual.ColumnDefinitions[4].DataType);
            Assert.True(actual.ColumnDefinitions[4].Nullable);
            Assert.Equal(new Identifier("fff"), actual.ColumnDefinitions[5].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_TEXT, actual.ColumnDefinitions[5].DataType);
            Assert.Equal(ColumnEncryptionFlags.Store, actual.ColumnDefinitions[5].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[5].Nullable);
            Assert.Equal(new Identifier("ggg"), actual.ColumnDefinitions[6].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_FLOAT, actual.ColumnDefinitions[6].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[6].EncryptionFlags);
            Assert.Null(actual.ColumnDefinitions[6].DefaultValue);
            Assert.Equal(new Identifier("hhh"), actual.ColumnDefinitions[7].ColumnName);
            Assert.Equal(SqlDataType.MSSQL_DATETIME, actual.ColumnDefinitions[7].DataType);
            Assert.Equal(ColumnEncryptionFlags.Store, actual.ColumnDefinitions[7].EncryptionFlags);
            Assert.Equal(new Identifier("CURRENT_TIMESTAMP"),
                ((ScalarFunction)actual.ColumnDefinitions[7].DefaultValue).FunctionName);
            Assert.True(actual.ColumnDefinitions[7].Nullable);
            Assert.True(actual.ColumnDefinitions[6].Nullable);
        }


        [Fact(DisplayName = "Parse PRISMADB EXPORT SETTINGS")]
        public void Parse_ExportSettings()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "PRISMADB EXPORT SETTINGS TO '/home/user/settings.json'";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (ExportSettingsCommand)result[0];
            Assert.Equal("/home/user/settings.json", actual.FileUri.strvalue);
        }

        [Fact(DisplayName = "Parse functions in SELECT")]
        public void Parse_Function()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "SELECT CONNECTION_ID()";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (SelectQuery)result[0];

            Assert.Equal(new Identifier("CONNECTION_ID"), ((ScalarFunction)actual.SelectExpressions[0]).FunctionName);
            Assert.Equal(new Identifier("CONNECTION_ID()"), ((ScalarFunction)actual.SelectExpressions[0]).Alias);
            Assert.Empty(((ScalarFunction)actual.SelectExpressions[0]).Parameters);

            Assert.Null(actual.Limit);
        }

        [Fact(DisplayName = "Parse functions w\\params in SELECT")]
        public void Parse_FunctionWithParams()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "SELECT TOP(1) COUNT(tt.col1) AS Num, TEST('string',12)";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (SelectQuery)result[0];

            Assert.Equal(new Identifier("COUNT"), ((ScalarFunction)actual.SelectExpressions[0]).FunctionName);
            Assert.Equal(new Identifier("Num"), ((ScalarFunction)actual.SelectExpressions[0]).Alias);
            Assert.Equal(new TableRef("tt"),
                ((ColumnRef)((ScalarFunction)actual.SelectExpressions[0]).Parameters[0]).Table);
            Assert.Equal(new Identifier("col1"),
                ((ColumnRef)((ScalarFunction)actual.SelectExpressions[0]).Parameters[0]).ColumnName);

            Assert.Equal(new Identifier("TEST"), ((ScalarFunction)actual.SelectExpressions[1]).FunctionName);
            Assert.Equal(new Identifier("TEST('string',12)"),
                ((ScalarFunction)actual.SelectExpressions[1]).Alias);
            Assert.Equal("string",
                (((ScalarFunction)actual.SelectExpressions[1]).Parameters[0] as StringConstant)?.strvalue);
            Assert.Equal(12, (((ScalarFunction)actual.SelectExpressions[1]).Parameters[1] as IntConstant)?.intvalue);

            Assert.Equal((uint)1, actual.Limit);
        }

        [Fact(DisplayName = "Parse INSERT INTO")]
        public void Parse_InsertInto()
        {
            // Setup
            var parser = new MsSqlParser();
            var test =
                "INSERT INTO [tt1] (tt1.col1, [tt1].col2, [tt1].[col3], tt1.[col4]) " +
                "VALUES ( -1, 12.345 , 'hey', 'hi' ), " +
                "(0,050, 3147483647, '  ', '&'), " +
                "(0xdec2976ac4fc39864683a83f7b9876f4b2cbc65b0b6ede9e74e9" +
                "cb918fda451597b0dffd6198943aef879acc3cdf426c61849299b7b" +
                "150d2589709d3d51752d4281b1f89ada432564e049bd3ab89fd2f9f" +
                "f1c24491d39a9afb94625d8b3d439d1cd391488850e4a6f638192fd" +
                "5e792b5d604024190be22c9a8c136349228311ab2321cde85a349c0" +
                "4c5222ef02acb3ef9e782062d390b1544df245d2c9590c2258b3e5a" +
                "90c5ba10dfe9daf4c9c8a340da149c2ca987616545c005ef4a607a5" +
                "14ecc35bb8f37b8ece, 0x4202, 0xffff)";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (InsertQuery)result[0];

            Assert.Equal(new TableRef("tt1"), actual.Into);
            Assert.Equal(new Identifier("col1"), actual.Columns[0].ColumnName);
            Assert.Equal(new TableRef("tt1"), actual.Columns[0].Table);
            Assert.Equal(new Identifier("col2"), actual.Columns[1].ColumnName);
            Assert.Equal(new TableRef("tt1"), actual.Columns[1].Table);
            Assert.Equal(new Identifier("col3"), actual.Columns[2].ColumnName);
            Assert.Equal(new TableRef("tt1"), actual.Columns[2].Table);
            Assert.Equal(new Identifier("col4"), actual.Columns[3].ColumnName);
            Assert.Equal(new TableRef("tt1"), actual.Columns[3].Table);
            Assert.Equal(3, actual.Values.Count);
            Assert.Equal(-1, (actual.Values[0][0] as IntConstant)?.intvalue);
            Assert.Equal(12.345m, (actual.Values[0][1] as FloatingPointConstant)?.floatvalue);
            Assert.Equal(50, (actual.Values[1][1] as IntConstant)?.intvalue);
            Assert.Equal(3147483647, (actual.Values[1][2] as IntConstant)?.intvalue);
            Assert.Equal("  ", (actual.Values[1][3] as StringConstant)?.strvalue);
            Assert.Equal("&", (actual.Values[1][4] as StringConstant)?.strvalue);
            Assert.Equal(typeof(BinaryConstant), actual.Values[2][0].GetType());
            Assert.Equal(new byte[] { 0x42, 0x02 }, (actual.Values[2][1] as BinaryConstant)?.binvalue);
            Assert.Equal(new byte[] { 0xff, 0xff }, (actual.Values[2][2] as BinaryConstant)?.binvalue);
        }

        [Fact(DisplayName = "Parse SELECT")]
        public void Parse_Select()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "SELECT (a+b)*(a+b), ((a+b)*(a+b)), (((a+b)*(a+b))) FROM t WHERE (a <= b) AND t.b !> a AND c IN ('abc', 'def') AND d NOT IN (123, 456) GROUP BY t.a, b ORDER BY a ASC, b DESC, c";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (SelectQuery)result[0];

            Assert.Equal("(a+b)*(a+b)", actual.SelectExpressions[0].Alias.id);
            Assert.Equal("((a+b)*(a+b))", actual.SelectExpressions[1].Alias.id);
            Assert.Equal("((a+b)*(a+b))", actual.SelectExpressions[2].Alias.id);

            Assert.Equal(new ColumnRef("b"), ((BooleanGreaterThan)actual.Where.CNF.AND[0].OR[0]).left);
            Assert.Equal(new ColumnRef("a"), ((BooleanGreaterThan)actual.Where.CNF.AND[0].OR[0]).right);
            Assert.Equal(new ColumnRef("a"), ((BooleanEquals)actual.Where.CNF.AND[0].OR[1]).left);
            Assert.Equal(new ColumnRef("b"), ((BooleanEquals)actual.Where.CNF.AND[0].OR[1]).right);
            Assert.False(((BooleanGreaterThan)actual.Where.CNF.AND[0].OR[0]).NOT);
            Assert.False(((BooleanEquals)actual.Where.CNF.AND[0].OR[1]).NOT);
            Assert.Equal(new ColumnRef("a"), ((BooleanGreaterThan)actual.Where.CNF.AND[1].OR[0]).left);
            Assert.Equal(new ColumnRef("t", "b"), ((BooleanGreaterThan)actual.Where.CNF.AND[1].OR[0]).right);
            Assert.Equal(new ColumnRef("t", "b"), ((BooleanEquals)actual.Where.CNF.AND[1].OR[1]).left);
            Assert.Equal(new ColumnRef("a"), ((BooleanEquals)actual.Where.CNF.AND[1].OR[1]).right);
            Assert.False(((BooleanGreaterThan)actual.Where.CNF.AND[1].OR[0]).NOT);
            Assert.False(((BooleanEquals)actual.Where.CNF.AND[1].OR[1]).NOT);
            Assert.Equal(new ColumnRef("c"), ((BooleanIn)actual.Where.CNF.AND[2].OR[0]).Column);
            Assert.Equal(new StringConstant("abc"), ((BooleanIn)actual.Where.CNF.AND[2].OR[0]).InValues[0]);
            Assert.Equal(new StringConstant("def"), ((BooleanIn)actual.Where.CNF.AND[2].OR[0]).InValues[1]);
            Assert.False(((BooleanIn)actual.Where.CNF.AND[2].OR[0]).NOT);
            Assert.Equal(new ColumnRef("d"), ((BooleanIn)actual.Where.CNF.AND[3].OR[0]).Column);
            Assert.Equal(new IntConstant(123), ((BooleanIn)actual.Where.CNF.AND[3].OR[0]).InValues[0]);
            Assert.Equal(new IntConstant(456), ((BooleanIn)actual.Where.CNF.AND[3].OR[0]).InValues[1]);
            Assert.True(((BooleanIn)actual.Where.CNF.AND[3].OR[0]).NOT);

            Assert.Equal(new ColumnRef("t", "a"), actual.GroupBy.GetColumns()[0]);
            Assert.Equal(new ColumnRef("b"), actual.GroupBy.GetColumns()[1]);

            Assert.Equal(new Identifier("a"), actual.OrderBy.OrderColumns[0].First.ColumnName);
            Assert.Equal(new Identifier("b"), actual.OrderBy.OrderColumns[1].First.ColumnName);
            Assert.Equal(new Identifier("c"), actual.OrderBy.OrderColumns[2].First.ColumnName);
            Assert.Equal(OrderDirection.ASC, actual.OrderBy.OrderColumns[0].Second);
            Assert.Equal(OrderDirection.DESC, actual.OrderBy.OrderColumns[1].Second);
            Assert.Equal(OrderDirection.ASC, actual.OrderBy.OrderColumns[2].Second);
        }

        [Fact(DisplayName = "Parse USE")]
        public void Parse_Use()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "USE ThisDB";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            var actual = (UseStatement)result[0];
            Assert.Equal(new DatabaseRef("ThisDB"), actual.Database);
        }

        [Fact(DisplayName = "Parse JOIN")]
        public void Parse_Join()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "select tt1.a AS abc, tt2.b FROM tt1 INNER JOIN tt2 ON tt1.c=tt2.c; " +
                       "select tt1.a, tt2.b FROM tt1 JOIN tt2 ON tt1.c=tt2.c WHERE tt1.a=123; " +
                       "select tt1.a, tt2.b FROM tt1 CROSS JOIN tt2 LEFT OUTER JOIN tt3 ON tt3.c=tt2.c;";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            {
                var actual = (SelectQuery)result[0];
                Assert.Equal(new ColumnRef("tt1", "a", "abc"), actual.SelectExpressions[0]);
                Assert.Equal(new ColumnRef("tt2", "b"), actual.SelectExpressions[1]);
                Assert.Equal(new TableRef("tt1"), actual.FromTables[0]);
                Assert.Equal(new TableRef("tt2"), actual.Joins[0].JoinTable);
                Assert.Equal(new ColumnRef("tt1", "c"), actual.Joins[0].FirstColumn);
                Assert.Equal(new ColumnRef("tt2", "c"), actual.Joins[0].SecondColumn);
                Assert.Equal(JoinType.INNER, actual.Joins[0].JoinType);
            }
            {
                var actual = (SelectQuery)result[1];
                Assert.Equal(new ColumnRef("tt1", "a"), actual.SelectExpressions[0]);
                Assert.Equal(new ColumnRef("tt2", "b"), actual.SelectExpressions[1]);
                Assert.Equal(new TableRef("tt1"), actual.FromTables[0]);
                Assert.Equal(new TableRef("tt2"), actual.Joins[0].JoinTable);
                Assert.Equal(new ColumnRef("tt1", "c"), actual.Joins[0].FirstColumn);
                Assert.Equal(new ColumnRef("tt2", "c"), actual.Joins[0].SecondColumn);
                Assert.Equal(new ColumnRef("tt1", "a"), ((BooleanEquals)actual.Where.CNF.AND[0].OR[0]).left);
                Assert.Equal(new IntConstant(123), ((BooleanEquals)actual.Where.CNF.AND[0].OR[0]).right);
                Assert.Equal(JoinType.INNER, actual.Joins[0].JoinType);
            }
            {
                var actual = (SelectQuery)result[2];
                Assert.Equal(new ColumnRef("tt1", "a"), actual.SelectExpressions[0]);
                Assert.Equal(new ColumnRef("tt2", "b"), actual.SelectExpressions[1]);
                Assert.Equal(new TableRef("tt1"), actual.FromTables[0]);
                Assert.Equal(new TableRef("tt2"), actual.Joins[0].JoinTable);
                Assert.Empty(actual.Joins[0].GetColumns());
                Assert.Equal(JoinType.CROSS, actual.Joins[0].JoinType);
                Assert.Equal(new TableRef("tt3"), actual.Joins[1].JoinTable);
                Assert.Equal(new ColumnRef("tt3", "c"), actual.Joins[1].FirstColumn);
                Assert.Equal(new ColumnRef("tt2", "c"), actual.Joins[1].SecondColumn);
                Assert.Equal(2, actual.Joins[1].GetColumns().Count);
                Assert.Equal(JoinType.LEFT_OUTER, actual.Joins[1].JoinType);
            }
        }

        [Fact(DisplayName = "Parse All Columns")]
        public void Parse_AllColumns()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "select * from tt; " +
                       "select t1.* from t1; ";

            // Act
            var result = parser.ParseToAst(test);

            // Assert
            {
                var actual = (SelectQuery)result[0];
                Assert.Equal(new AllColumns(), actual.SelectExpressions[0]);
            }
            {
                var actual = (SelectQuery)result[1];
                Assert.Equal(new AllColumns("t1"), actual.SelectExpressions[0]);
            }
        }

        [Fact(DisplayName = "Parse NULLs")]
        public void Parse_NULLExpressions()
        {
            // Setup
            var parser = new MsSqlParser();
            var test1 = "SELECT NULL";
            var test2 = "INSERT INTO tbl1 ( col1 ) VALUES ( NULL )";
            var test3 = "SELECT * FROM tbl1 WHERE col1 IS NOT NULL AND col2 IS NULL";

            // Act
            var result1 = parser.ParseToAst(test1)[0];

            // Assert
            Assert.IsType<SelectQuery>(result1);
            Assert.IsType<NullConstant>(((SelectQuery)result1).SelectExpressions[0]);

            // Act
            var result2 = parser.ParseToAst(test2)[0];

            // Assert
            Assert.IsType<InsertQuery>(result2);
            Assert.IsType<NullConstant>(((InsertQuery)result2).Values[0][0]);

            // Act
            var result3 = parser.ParseToAst(test3)[0];

            // Assert
            Assert.IsType<SelectQuery>(result3);
            Assert.True(((BooleanIsNull)((SelectQuery)result3).Where.CNF.AND[0].OR[0]).NOT);
            Assert.False(((BooleanIsNull)((SelectQuery)result3).Where.CNF.AND[1].OR[0]).NOT);
        }

        [Fact(DisplayName = "Parse known functions")]
        public void Parse_KnownFuncs()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "SELECT RandomFunc(), SuM(col1), CoUNt(col2), coUNT(*), avg (col3)";

            // Act
            var result = parser.ParseToAst(test)[0] as SelectQuery;

            // Assert
            Assert.IsType<ScalarFunction>(result.SelectExpressions[0]);
            Assert.IsNotType<SumAggregationFunction>(result.SelectExpressions[0]);
            Assert.IsNotType<AvgAggregationFunction>(result.SelectExpressions[0]);
            Assert.IsNotType<CountAggregationFunction>(result.SelectExpressions[0]);

            Assert.IsType<SumAggregationFunction>(result.SelectExpressions[1]);
            Assert.IsType<ColumnRef>((result.SelectExpressions[1] as ScalarFunction).Parameters[0]);

            Assert.IsType<CountAggregationFunction>(result.SelectExpressions[2]);
            Assert.IsType<ColumnRef>((result.SelectExpressions[2] as ScalarFunction).Parameters[0]);

            Assert.IsType<CountAggregationFunction>(result.SelectExpressions[3]);
            Assert.Empty((result.SelectExpressions[3] as ScalarFunction).Parameters);

            Assert.IsType<AvgAggregationFunction>(result.SelectExpressions[4]);
            Assert.IsType<ColumnRef>((result.SelectExpressions[4] as ScalarFunction).Parameters[0]);
        }

        [Fact(DisplayName = "Parse UPDATE")]
        public void Parse_Update()
        {
            // Setup
            var parser = new MsSqlParser();
            var test = "UPDATE tt SET a = NULL WHERE b = 'abc'; ";

            // Act
            var result = parser.ParseToAst(test)[0] as UpdateQuery;

            // Assert
            Assert.IsType<ColumnRef>(result.UpdateExpressions[0].First);
            Assert.IsType<NullConstant>(result.UpdateExpressions[0].Second);
        }
    }
}