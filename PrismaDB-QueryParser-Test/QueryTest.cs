using Microsoft.VisualStudio.TestTools.UnitTesting;
using PrismaDB.QueryParser;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;

namespace PrismaDB_QueryParser_Test
{
    [TestClass]
    public class QueryTest
    {
        [TestMethod]
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

            Assert.AreEqual(actual.TableName, new TableRef("ttt"));
            Assert.AreEqual(actual.ColumnDefinitions[0].ColumnName, new Identifier("aaa"));
            Assert.AreEqual(actual.ColumnDefinitions[0].DataType, SQLDataType.INT);
            Assert.AreEqual(actual.ColumnDefinitions[0].EncryptionFlags, ColumnEncryptionFlags.IntegerAddition | ColumnEncryptionFlags.IntegerMultiplication);
            Assert.AreEqual(actual.ColumnDefinitions[0].Nullable, false);
            Assert.AreEqual(actual.ColumnDefinitions[1].ColumnName, new Identifier("bbb"));
            Assert.AreEqual(actual.ColumnDefinitions[1].DataType, SQLDataType.INT);
            Assert.AreEqual(actual.ColumnDefinitions[1].EncryptionFlags, ColumnEncryptionFlags.None);
            Assert.AreEqual(actual.ColumnDefinitions[1].Nullable, true);
            Assert.AreEqual(actual.ColumnDefinitions[2].ColumnName, new Identifier("ccc"));
            Assert.AreEqual(actual.ColumnDefinitions[2].DataType, SQLDataType.VARCHAR);
            Assert.AreEqual(actual.ColumnDefinitions[2].Length, 80);
            Assert.AreEqual(actual.ColumnDefinitions[2].EncryptionFlags, ColumnEncryptionFlags.None);
            Assert.AreEqual(actual.ColumnDefinitions[2].Nullable, false);
            Assert.AreEqual(actual.ColumnDefinitions[3].ColumnName, new Identifier("ddd"));
            Assert.AreEqual(actual.ColumnDefinitions[3].DataType, SQLDataType.VARCHAR);
            Assert.AreEqual(actual.ColumnDefinitions[3].Length, 20);
            Assert.AreEqual(actual.ColumnDefinitions[3].EncryptionFlags, ColumnEncryptionFlags.Text | ColumnEncryptionFlags.Search);
            Assert.AreEqual(actual.ColumnDefinitions[3].Nullable, true);
        }

        [TestMethod]
        public void Parse_InsertInto()
        {
            // Setup
            var parser = new SqlParser();
            var test = "INSERT INTO [tt1] (tt1.col1, [tt1].col2, [tt1].[col3], tt1.[col4]) VALUES ( 1, 2 , 'hey', 'hi' ), (0,050,'  ', '&')";

            // Act
            var result = parser.ParseToAST(test);

            // Assert
            var actual = (InsertQuery)result[0];

            Assert.AreEqual(actual.Into, new TableRef("tt1"));
            Assert.AreEqual(actual.Columns[0].ColumnName, new Identifier("col1"));
            Assert.AreEqual(actual.Columns[0].Table, new TableRef("tt1"));
            Assert.AreEqual(actual.Columns[1].ColumnName, new Identifier("col2"));
            Assert.AreEqual(actual.Columns[1].Table, new TableRef("tt1"));
            Assert.AreEqual(actual.Columns[2].ColumnName, new Identifier("col3"));
            Assert.AreEqual(actual.Columns[2].Table, new TableRef("tt1"));
            Assert.AreEqual(actual.Columns[3].ColumnName, new Identifier("col4"));
            Assert.AreEqual(actual.Columns[3].Table, new TableRef("tt1"));
            Assert.AreEqual(actual.Values.Count, 2);
            Assert.AreEqual((actual.Values[1][1] as IntConstant)?.intvalue, 50);
            Assert.AreEqual((actual.Values[1][2] as StringConstant)?.strvalue, "  ");
            Assert.AreEqual((actual.Values[1][3] as StringConstant)?.strvalue, "&");
        }
    }
}
