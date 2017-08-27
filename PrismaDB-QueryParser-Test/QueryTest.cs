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
        [Fact]
        public void Parse_CreateTable_WithPartialEncryption()
        {
            // Setup
            var parser = new SqlParser();
            var test = "CREATE TABLE ttt " +
                       "(aaa INT ENCRYPTED FOR (INTEGER_ADDITION, INTEGER_MULTIPLICATION) NOT NULL, " +
                       "[bbb] INT NULL, " +
                       "ccc VARCHAR(80) NOT NULL, " +
                       "ddd VARCHAR(20) ENCRYPTED FOR (TEXT, SEARCH))";

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
            Assert.Equal(ColumnEncryptionFlags.Text | ColumnEncryptionFlags.Search, actual.ColumnDefinitions[3].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[3].Nullable);
        }

        [Fact]
        public void Parse_InsertInto()
        {
            // Setup
            var parser = new SqlParser();
            var test = "INSERT INTO [tt1] (tt1.col1, [tt1].col2, [tt1].[col3], tt1.[col4]) VALUES ( 1, 2 , 'hey', 'hi' ), (0,050,'  ', '&')";

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
            Assert.Equal(50, (actual.Values[1][1] as IntConstant)?.intvalue);
            Assert.Equal("  ", (actual.Values[1][2] as StringConstant)?.strvalue);
            Assert.Equal("&", (actual.Values[1][3] as StringConstant)?.strvalue);
        }

        [Fact]
        public void Parse_Use()
        {
            // Setup
            var parser = new SqlParser();
            var test = "USE ThisDB";

            // Act
            var ex = Assert.Throws<NotSupportedException>(() => parser.ParseToAST(test));
            Assert.Equal("Database switching not supported.", ex.Message);
        }
    }
}
