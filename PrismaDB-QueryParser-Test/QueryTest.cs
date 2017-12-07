using System;
using Xunit;
using PrismaDB.QueryParser;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;

namespace PrismaDB_QueryParser_Test
{
    public class QueryTest
    {
        [Fact(DisplayName = "Parse CREATE TABLE w\\partial encryption")]
        public void Parse_CreateTable_WithPartialEncryption()
        {
            // Setup
            var parser = new SqlParser();
            var test = "CREATE TABLE ttt " +
                       "(aaa INT ENCRYPTED FOR (INTEGER_ADDITION, INTEGER_MULTIPLICATION) NOT NULL, " +
                       "[bbb] INT NULL, " +
                       "ccc VARCHAR(80) NOT NULL, " +
                       "ddd VARCHAR(20) ENCRYPTED FOR (STORE, SEARCH), " +
                       "eee TEXT NULL, " +
                       "fff TEXT ENCRYPTED NULL, " +
                       "ggg FLOAT" + ")";

            // Act
            var result = parser.ParseToAST(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("ttt"), actual.TableName);
            Assert.Equal(new Identifier("aaa"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SQLDataType.INT, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.IntegerAddition | ColumnEncryptionFlags.IntegerMultiplication, actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.False(actual.ColumnDefinitions[0].Nullable);
            Assert.Equal(new Identifier("bbb"), actual.ColumnDefinitions[1].ColumnName);
            Assert.Equal(SQLDataType.INT, actual.ColumnDefinitions[1].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[1].Nullable);
            Assert.Equal(new Identifier("ccc"), actual.ColumnDefinitions[2].ColumnName);
            Assert.Equal(SQLDataType.VARCHAR, actual.ColumnDefinitions[2].DataType);
            Assert.Equal(80, actual.ColumnDefinitions[2].Length);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[2].EncryptionFlags);
            Assert.False(actual.ColumnDefinitions[2].Nullable);
            Assert.Equal(new Identifier("ddd"), actual.ColumnDefinitions[3].ColumnName);
            Assert.Equal(SQLDataType.VARCHAR, actual.ColumnDefinitions[3].DataType);
            Assert.Equal(20, actual.ColumnDefinitions[3].Length);
            Assert.Equal(ColumnEncryptionFlags.Store | ColumnEncryptionFlags.Search, actual.ColumnDefinitions[3].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[3].Nullable);
            Assert.Equal(new Identifier("eee"), actual.ColumnDefinitions[4].ColumnName);
            Assert.Equal(SQLDataType.TEXT, actual.ColumnDefinitions[4].DataType);
            Assert.True(actual.ColumnDefinitions[4].Nullable);
            Assert.Equal(new Identifier("fff"), actual.ColumnDefinitions[5].ColumnName);
            Assert.Equal(SQLDataType.TEXT, actual.ColumnDefinitions[5].DataType);
            Assert.Equal(ColumnEncryptionFlags.Store, actual.ColumnDefinitions[5].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[5].Nullable);
            Assert.Equal(new Identifier("ggg"), actual.ColumnDefinitions[6].ColumnName);
            Assert.Equal(SQLDataType.DOUBLE, actual.ColumnDefinitions[6].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[6].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[6].Nullable);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\TEXT")]
        public void Parse_CreateTable_TEXT()
        {
            // Setup
            var parser = new SqlParser();
            var test = "CREATE TABLE table1 " +
                       "(col1 TEXT, " +
                       "col2 TEXT ENCRYPTED FOR (STORE, SEARCH) NULL)";

            // Act
            var result = parser.ParseToAST(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);

            Assert.Equal(new Identifier("col1"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SQLDataType.TEXT, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[0].Nullable);
            Assert.Null(actual.ColumnDefinitions[0].Length);

            Assert.Equal(new Identifier("col2"), actual.ColumnDefinitions[1].ColumnName);
            Assert.Equal(SQLDataType.TEXT, actual.ColumnDefinitions[1].DataType);
            Assert.NotEqual(ColumnEncryptionFlags.None, ColumnEncryptionFlags.Store & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.NotEqual(ColumnEncryptionFlags.None, ColumnEncryptionFlags.Search & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None, ColumnEncryptionFlags.IntegerAddition & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None, ColumnEncryptionFlags.IntegerMultiplication & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[1].Nullable);
            Assert.Null(actual.ColumnDefinitions[1].Length);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\DATETIME")]
        public void Parse_CreateTable_DATETIME()
        {
            // Setup
            var parser = new SqlParser();
            var test = "CREATE TABLE table1 " +
                       "(col1 DATETIME NOT NULL)";

            // Act
            var result = parser.ParseToAST(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);

            Assert.Equal(new Identifier("col1"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SQLDataType.DATETIME, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.False(actual.ColumnDefinitions[0].Nullable);
            Assert.Null(actual.ColumnDefinitions[0].Length);
        }

        [Fact(DisplayName = "Parse INSERT INTO")]
        public void Parse_InsertInto()
        {
            // Setup
            var parser = new SqlParser();
            var test = "INSERT INTO [tt1] (tt1.col1, [tt1].col2, [tt1].[col3], tt1.[col4]) VALUES ( 1, 12.345 , 'hey', 'hi' ), (0,050,'  ', '&')";

            // Act
            var result = parser.ParseToAST(test);

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
            Assert.Equal(2, actual.Values.Count);
            Assert.Equal("12.345", (actual.Values[0][1] as StringConstant)?.strvalue);
            Assert.Equal(50, (actual.Values[1][1] as IntConstant)?.intvalue);
            Assert.Equal("  ", (actual.Values[1][2] as StringConstant)?.strvalue);
            Assert.Equal("&", (actual.Values[1][3] as StringConstant)?.strvalue);
        }

        [Fact(DisplayName = "Parse USE")]
        public void Parse_Use()
        {
            // Setup
            var parser = new SqlParser();
            var test = "USE ThisDB";

            // Act
            var ex = Assert.Throws<NotSupportedException>(() => parser.ParseToAST(test));
            Assert.Equal("Database switching not supported.", ex.Message);
        }

        [Fact(DisplayName = "Parse functions in SELECT")]
        public void Parse_Function()
        {
            // Setup
            var parser = new SqlParser();
            var test = "SELECT CONNECTION_ID()";

            // Act
            var result = parser.ParseToAST(test);

            // Assert
            var actual = (SelectQuery)result[0];

            Assert.Equal(new Identifier("CONNECTION_ID"), ((ScalarFunction)actual.SelectExpressions[0]).FunctionName);
            Assert.Equal(new Identifier("CONNECTION_ID()"), ((ScalarFunction)actual.SelectExpressions[0]).ColumnName);
            Assert.Empty(((ScalarFunction)actual.SelectExpressions[0]).Parameters);

            Assert.Null(actual.Limit);
        }

        [Fact(DisplayName = "Parse functions w\\params in SELECT")]
        public void Parse_FunctionWithParams()
        {
            // Setup
            var parser = new SqlParser();
            var test = "SELECT TOP(1) COUNT(tt.col1) AS Num, TEST('string',12)";

            // Act
            var result = parser.ParseToAST(test);

            // Assert
            var actual = (SelectQuery)result[0];

            Assert.Equal(new Identifier("COUNT"), ((ScalarFunction)actual.SelectExpressions[0]).FunctionName);
            Assert.Equal(new Identifier("Num"), ((ScalarFunction)actual.SelectExpressions[0]).ColumnName);
            Assert.Equal(new TableRef("tt"), ((ColumnRef)((ScalarFunction)actual.SelectExpressions[0]).Parameters[0]).Table);
            Assert.Equal(new Identifier("col1"), ((ColumnRef)((ScalarFunction)actual.SelectExpressions[0]).Parameters[0]).ColumnName);

            Assert.Equal(new Identifier("TEST"), ((ScalarFunction)actual.SelectExpressions[1]).FunctionName);
            Assert.Equal(new Identifier("TEST('string',12)"), ((ScalarFunction)actual.SelectExpressions[1]).ColumnName);
            Assert.Equal("string", (((ScalarFunction)actual.SelectExpressions[1]).Parameters[0] as StringConstant)?.strvalue);
            Assert.Equal(12, (((ScalarFunction)actual.SelectExpressions[1]).Parameters[1] as IntConstant)?.intvalue);

            Assert.Equal((uint)1, actual.Limit);
        }

        [Fact(DisplayName = "Parse ALTER TABLE")]
        public void Parse_AlterTable()
        {
            // Setup
            var parser = new SqlParser();
            var test = "ALTER TABLE table1 " +
                       "ALTER COLUMN col1 TEXT ENCRYPTED FOR (STORE, SEARCH) NULL";

            // Act
            var result = parser.ParseToAST(test);

            // Assert
            var actual = (AlterTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);
            Assert.Equal(AlterType.MODIFY, actual.AlterType);

            Assert.Equal(new Identifier("col1"), actual.AlteredColumns[0].ColumnDefinition.ColumnName);
            Assert.Equal(SQLDataType.TEXT, actual.AlteredColumns[0].ColumnDefinition.DataType);
            Assert.NotEqual(ColumnEncryptionFlags.None, ColumnEncryptionFlags.Store & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.NotEqual(ColumnEncryptionFlags.None, ColumnEncryptionFlags.Search & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None, ColumnEncryptionFlags.IntegerAddition & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None, ColumnEncryptionFlags.IntegerMultiplication & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.True(actual.AlteredColumns[0].ColumnDefinition.Nullable);
            Assert.Null(actual.AlteredColumns[0].ColumnDefinition.Length);
        }
    }
}
