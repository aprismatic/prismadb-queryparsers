using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using PrismaDB.QueryParser;
using System.Collections.Generic;
using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DDL;

namespace PrismaDB_QueryParser_Test
{
    [TestClass]
    public class QueryTest
    {

        [TestMethod]
        public void Parse_CreateTable_WithPartialEncryption()
        {
            // Setup
            SqlParser parser;
            parser = new SqlParser();
            string test = "CREATE TABLE ttt "+
                "(aaa INT ENCRYPTED FOR (INTEGER_ADDITION, integer_multiplication) NOT NULL, "+
                "bbb INT NULL, "+
                "ccc VARCHAR(80) NOT NULL, " +
                "ddd VARCHAR(20) ENCRYPTED FOR (TEXT, search))";

            // Act
            List<Query> result = parser.ParseToAST(test);

            // Assert  
            CreateTableQuery actual = (CreateTableQuery)result[0];

            Assert.AreEqual(actual.TableName.TableName, new TableRef("ttt").TableName);
            Assert.AreEqual(actual.ColumnDefinitions[0].ColumnName, "aaa");
            Assert.AreEqual(actual.ColumnDefinitions[0].DataType, SQLDataType.INT);
            Assert.AreEqual(actual.ColumnDefinitions[0].EncryptionFlags, ColumnEncryptionFlags.IntegerAddition | ColumnEncryptionFlags.IntegerMultiplication);
            Assert.AreEqual(actual.ColumnDefinitions[0].Nullable, false);
            Assert.AreEqual(actual.ColumnDefinitions[1].ColumnName, "bbb");
            Assert.AreEqual(actual.ColumnDefinitions[1].DataType, SQLDataType.INT);
            Assert.AreEqual(actual.ColumnDefinitions[1].EncryptionFlags, ColumnEncryptionFlags.None);
            Assert.AreEqual(actual.ColumnDefinitions[1].Nullable, true);
            Assert.AreEqual(actual.ColumnDefinitions[2].ColumnName, "ccc");
            Assert.AreEqual(actual.ColumnDefinitions[2].DataType, SQLDataType.VARCHAR);
            Assert.AreEqual(actual.ColumnDefinitions[2].Length, 80);
            Assert.AreEqual(actual.ColumnDefinitions[2].EncryptionFlags, ColumnEncryptionFlags.None);
            Assert.AreEqual(actual.ColumnDefinitions[2].Nullable, false);
            Assert.AreEqual(actual.ColumnDefinitions[3].ColumnName, "ddd");
            Assert.AreEqual(actual.ColumnDefinitions[3].DataType, SQLDataType.VARCHAR);
            Assert.AreEqual(actual.ColumnDefinitions[3].Length, 20);
            Assert.AreEqual(actual.ColumnDefinitions[3].EncryptionFlags, ColumnEncryptionFlags.Text | ColumnEncryptionFlags.Search);
            Assert.AreEqual(actual.ColumnDefinitions[3].Nullable, true);
        }


       
    }
}
