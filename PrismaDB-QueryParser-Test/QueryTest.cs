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

            Assert.Equal(actual.TableName, new TableRef("ttt"));
            Assert.Equal(actual.ColumnDefinitions[0].ColumnName, new Identifier("aaa"));
            Assert.Equal(actual.ColumnDefinitions[0].DataType, SQLDataType.INT);
            Assert.Equal(actual.ColumnDefinitions[0].EncryptionFlags, ColumnEncryptionFlags.IntegerAddition | ColumnEncryptionFlags.IntegerMultiplication);
            Assert.Equal(actual.ColumnDefinitions[0].Nullable, false);
            Assert.Equal(actual.ColumnDefinitions[1].ColumnName, new Identifier("bbb"));
            Assert.Equal(actual.ColumnDefinitions[1].DataType, SQLDataType.INT);
            Assert.Equal(actual.ColumnDefinitions[1].EncryptionFlags, ColumnEncryptionFlags.None);
            Assert.Equal(actual.ColumnDefinitions[1].Nullable, true);
            Assert.Equal(actual.ColumnDefinitions[2].ColumnName, new Identifier("ccc"));
            Assert.Equal(actual.ColumnDefinitions[2].DataType, SQLDataType.VARCHAR);
            Assert.Equal(actual.ColumnDefinitions[2].Length, 80);
            Assert.Equal(actual.ColumnDefinitions[2].EncryptionFlags, ColumnEncryptionFlags.None);
            Assert.Equal(actual.ColumnDefinitions[2].Nullable, false);
            Assert.Equal(actual.ColumnDefinitions[3].ColumnName, new Identifier("ddd"));
            Assert.Equal(actual.ColumnDefinitions[3].DataType, SQLDataType.VARCHAR);
            Assert.Equal(actual.ColumnDefinitions[3].Length, 20);
            Assert.Equal(actual.ColumnDefinitions[3].EncryptionFlags, ColumnEncryptionFlags.Text | ColumnEncryptionFlags.Search);
            Assert.Equal(actual.ColumnDefinitions[3].Nullable, true);
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

            Assert.Equal(actual.Into, new TableRef("tt1"));
            Assert.Equal(actual.Columns[0].ColumnName, new Identifier("col1"));
            Assert.Equal(actual.Columns[0].Table, new TableRef("tt1"));
            Assert.Equal(actual.Columns[1].ColumnName, new Identifier("col2"));
            Assert.Equal(actual.Columns[1].Table, new TableRef("tt1"));
            Assert.Equal(actual.Columns[2].ColumnName, new Identifier("col3"));
            Assert.Equal(actual.Columns[2].Table, new TableRef("tt1"));
            Assert.Equal(actual.Columns[3].ColumnName, new Identifier("col4"));
            Assert.Equal(actual.Columns[3].Table, new TableRef("tt1"));
            Assert.Equal(actual.Values.Count, 2);
            Assert.Equal((actual.Values[1][1] as IntConstant)?.intvalue, 50);
            Assert.Equal((actual.Values[1][2] as StringConstant)?.strvalue, "  ");
            Assert.Equal((actual.Values[1][3] as StringConstant)?.strvalue, "&");
        }

        [Fact]
        public void Parse_Use()
        {
            // Setup
            var parser = new SqlParser();
            var test = "USE ThisDB";

            // Act
            var ex = Assert.Throws<NotSupportedException>(() => parser.ParseToAST(test));
            Assert.Equal(ex.Message, "Database switching not supported.");
        }
    }
}
