using PrismaDB.QueryAST;
using PrismaDB.QueryAST.DCL;
using PrismaDB.QueryAST.DDL;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryParser.MySQL;
using Xunit;

namespace ParserTests
{
    public class QueryTest
    {
        [Fact(DisplayName = "Parse ALTER TABLE")]
        public void Parse_AlterTable()
        {
            // Setup
            var test = "ALTER TABLE table1 " +
                       "MODIFY COLUMN col1 TEXT ENCRYPTED FOR (STORE, SEARCH) NULL";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (AlterTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);
            Assert.Equal(AlterType.MODIFY, actual.AlterType);

            Assert.Equal(new Identifier("col1"), actual.AlteredColumns[0].ColumnDefinition.ColumnName);
            Assert.Equal(SqlDataType.MySQL_TEXT, actual.AlteredColumns[0].ColumnDefinition.DataType);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Store & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Search & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Addition & actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Multiplication &
                actual.AlteredColumns[0].ColumnDefinition.EncryptionFlags);
            Assert.True(actual.AlteredColumns[0].ColumnDefinition.Nullable);
            Assert.Null(actual.AlteredColumns[0].ColumnDefinition.Length);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\DATETIME")]
        public void Parse_CreateTable_DATETIME()
        {
            // Setup
            var test = "CREATE TABLE table1 " +
                       "(col1 DATETIME NOT NULL)";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);

            Assert.Equal(new Identifier("col1"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SqlDataType.MySQL_DATETIME, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.False(actual.ColumnDefinitions[0].Nullable);
            Assert.Null(actual.ColumnDefinitions[0].Length);
        }

        [Fact(DisplayName = "Parse NULLs")]
        public void Parse_NULLExpressions()
        {
            // Setup
            var test1 = "SELECT NULL";
            var test2 = "INSERT INTO tbl1 ( col1 ) VALUES ( NULL )";
            var test3 = "SELECT * FROM tbl1 WHERE col1 IS NOT NULL AND col2 IS NULL";

            // Act
            var result1 = MySqlQueryParser.ParseToAst(test1)[0];

            // Assert
            Assert.IsType<SelectQuery>(result1);
            Assert.IsType<NullConstant>(((ConstantContainer)((SelectQuery)result1).SelectExpressions[0]).constant);

            // Act
            var result2 = MySqlQueryParser.ParseToAst(test2)[0];

            // Assert
            Assert.IsType<InsertQuery>(result2);
            Assert.IsType<NullConstant>(((ConstantContainer)((InsertQuery)result2).Values[0][0]).constant);

            // Act
            var result3 = MySqlQueryParser.ParseToAst(test3)[0];

            // Assert
            Assert.IsType<SelectQuery>(result3);
            Assert.True(((BooleanIsNull)((SelectQuery)result3).Where.CNF.AND[0].OR[0]).NOT);
            Assert.False(((BooleanIsNull)((SelectQuery)result3).Where.CNF.AND[1].OR[0]).NOT);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\TEXT")]
        public void Parse_CreateTable_TEXT()
        {
            // Setup
            var test = "CREATE TABLE table1 " +
                       "(col1 TEXT, " +
                       "col2 TEXT ENCRYPTED FOR (STORE, SEARCH) NULL)";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("table1"), actual.TableName);

            Assert.Equal(new Identifier("col1"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SqlDataType.MySQL_TEXT, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[0].Nullable);
            Assert.Null(actual.ColumnDefinitions[0].Length);

            Assert.Equal(new Identifier("col2"), actual.ColumnDefinitions[1].ColumnName);
            Assert.Equal(SqlDataType.MySQL_TEXT, actual.ColumnDefinitions[1].DataType);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Store & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.NotEqual(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Search & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Addition & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.Equal(ColumnEncryptionFlags.None,
                ColumnEncryptionFlags.Multiplication & actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[1].Nullable);
            Assert.Null(actual.ColumnDefinitions[1].Length);
        }

        [Fact(DisplayName = "Parse CREATE TABLE w\\partial encryption")]
        public void Parse_CreateTable_WithPartialEncryption()
        {
            // Setup
            var test = "CREATE TABLE ttt " +
                       "(aaa INT ENCRYPTED FOR (ADDITION, MULTIPLICATION) NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                       "`bbb` BIGINT NULL, " +
                       "ccc VARCHAR(80) NOT NULL, " +
                       "ddd VARCHAR(20) ENCRYPTED FOR (STORE, SEARCH), " +
                       "eee TEXT NULL, " +
                       "fff TEXT ENCRYPTED NULL, " +
                       "ggg DOUBLE, " +
                       "hhh ENUM('ABC', 'def') ENCRYPTED NULL, " +
                       "iii TIMESTAMP ENCRYPTED DEFAULT CURRENT_TIMESTAMP" + ")";
            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (CreateTableQuery)result[0];

            Assert.Equal(new TableRef("ttt"), actual.TableName);
            Assert.Equal(new Identifier("aaa"), actual.ColumnDefinitions[0].ColumnName);
            Assert.Equal(SqlDataType.MySQL_INT, actual.ColumnDefinitions[0].DataType);
            Assert.Equal(ColumnEncryptionFlags.Addition | ColumnEncryptionFlags.Multiplication,
                actual.ColumnDefinitions[0].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[0].AutoIncrement);
            Assert.False(actual.ColumnDefinitions[0].Nullable);
            Assert.Equal(new Identifier("bbb"), actual.ColumnDefinitions[1].ColumnName);
            Assert.Equal(SqlDataType.MySQL_BIGINT, actual.ColumnDefinitions[1].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[1].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[1].Nullable);
            Assert.False(actual.ColumnDefinitions[1].AutoIncrement);
            Assert.Equal(new Identifier("ccc"), actual.ColumnDefinitions[2].ColumnName);
            Assert.Equal(SqlDataType.MySQL_VARCHAR, actual.ColumnDefinitions[2].DataType);
            Assert.Equal(80, actual.ColumnDefinitions[2].Length);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[2].EncryptionFlags);
            Assert.False(actual.ColumnDefinitions[2].Nullable);
            Assert.Equal(new Identifier("ddd"), actual.ColumnDefinitions[3].ColumnName);
            Assert.Equal(SqlDataType.MySQL_VARCHAR, actual.ColumnDefinitions[3].DataType);
            Assert.Equal(20, actual.ColumnDefinitions[3].Length);
            Assert.Equal(ColumnEncryptionFlags.Store | ColumnEncryptionFlags.Search,
                actual.ColumnDefinitions[3].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[3].Nullable);
            Assert.Equal(new Identifier("eee"), actual.ColumnDefinitions[4].ColumnName);
            Assert.Equal(SqlDataType.MySQL_TEXT, actual.ColumnDefinitions[4].DataType);
            Assert.True(actual.ColumnDefinitions[4].Nullable);
            Assert.Equal(new Identifier("fff"), actual.ColumnDefinitions[5].ColumnName);
            Assert.Equal(SqlDataType.MySQL_TEXT, actual.ColumnDefinitions[5].DataType);
            Assert.Equal(ColumnEncryptionFlags.Store, actual.ColumnDefinitions[5].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[5].Nullable);
            Assert.Equal(new Identifier("ggg"), actual.ColumnDefinitions[6].ColumnName);
            Assert.Equal(SqlDataType.MySQL_DOUBLE, actual.ColumnDefinitions[6].DataType);
            Assert.Equal(ColumnEncryptionFlags.None, actual.ColumnDefinitions[6].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[6].Nullable);
            Assert.Equal(new Identifier("hhh"), actual.ColumnDefinitions[7].ColumnName);
            Assert.Equal(SqlDataType.MySQL_ENUM, actual.ColumnDefinitions[7].DataType);
            Assert.Equal(ColumnEncryptionFlags.Store, actual.ColumnDefinitions[7].EncryptionFlags);
            Assert.True(actual.ColumnDefinitions[7].Nullable);
            Assert.Null(actual.ColumnDefinitions[7].DefaultValue);
            Assert.Equal(new Identifier("iii"), actual.ColumnDefinitions[8].ColumnName);
            Assert.Equal(SqlDataType.MySQL_TIMESTAMP, actual.ColumnDefinitions[8].DataType);
            Assert.Equal(ColumnEncryptionFlags.Store, actual.ColumnDefinitions[8].EncryptionFlags);
            Assert.Equal(new Identifier("CURRENT_TIMESTAMP"),
                ((ScalarFunction)actual.ColumnDefinitions[8].DefaultValue).FunctionName);
            Assert.True(actual.ColumnDefinitions[7].Nullable);
            Assert.Equal(new StringConstant("ABC"), actual.ColumnDefinitions[7].EnumValues[0]);
            Assert.Equal(new StringConstant("def"), actual.ColumnDefinitions[7].EnumValues[1]);
        }

        [Fact(DisplayName = "Parse CREATE INDEX")]
        public void Parse_CreateIndex()
        {
            // Setup
            var test = "CREATE INDEX i1 ON TT (a, b, c)";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (CreateIndexQuery)result[0];
            Assert.Equal(IndexModifier.DEFAULT, actual.Modifier);
            Assert.Equal(IndexType.DEFAULT, actual.Type);
            Assert.Equal(new TableRef("TT"), actual.OnTable);
            Assert.Equal(new Identifier("i1"), actual.Name);
            Assert.Equal(new ColumnRef("a"), actual.OnColumns[0]);
            Assert.Equal(new ColumnRef("b"), actual.OnColumns[1]);
            Assert.Equal(new ColumnRef("c"), actual.OnColumns[2]);
        }

        [Fact(DisplayName = "Parse Commands")]
        public void Parse_Commands()
        {
            // Setup 
            var test = "PRISMADB EXPORT KEYS TO '/home/user/settings.json';" +
                       "PRISMADB REGISTER USER 'sherlock' PASSWORD '@22!B';" +
                       "PRISMADB UPDATE KEYS;" +
                       "PRISMADB DECRYPT tt.col1;" +
                       "PRISMADB ENCRYPT tt.col1;" +
                       "PRISMADB ENCRYPT tt.col1 FOR (STORE, SEARCH);" +
                       "PRISMADB DECRYPT tt.col1 STATUS;" +
                       "PRISMADB REBALANCE OPETREE;" +
                       "PRISMADB REBALANCE OPETREE WITH VALUES (1, 2);" +
                       "PRISMADB SAVE OPETREE;" +
                       "PRISMADB LOAD OPETREE;" +
                       "PRISMADB LOAD SCHEMA;" +
                       "PRISMADB BYPASS SELECT * FROM tt;" +
                       "PRISMADB REFRESH LICENSE;" +
                       "PRISMADB SET LICENSE KEY 'abc';" +
                       "PRISMADB CHECK LICENSE STATUS;";

            // Act 
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert 
            Assert.Equal("/home/user/settings.json", ((ExportKeysCommand)result[0]).FileUri.strvalue);
            Assert.Equal("sherlock", ((RegisterUserCommand)result[1]).UserId.strvalue);
            Assert.Equal("@22!B", ((RegisterUserCommand)result[1]).Password.strvalue);
            Assert.Equal(typeof(UpdateKeysCommand), result[2].GetType());
            Assert.False(((UpdateKeysCommand)result[2]).StatusCheck);
            Assert.Equal(new ColumnRef("tt", "col1"), ((DecryptColumnCommand)result[3]).Column);
            Assert.False(((DecryptColumnCommand)result[3]).StatusCheck);
            Assert.Equal(new ColumnRef("tt", "col1"), ((EncryptColumnCommand)result[4]).Column);
            Assert.Equal(ColumnEncryptionFlags.Store, ((EncryptColumnCommand)result[4]).EncryptionFlags);
            Assert.True(((EncryptColumnCommand)result[5]).EncryptionFlags.HasFlag(ColumnEncryptionFlags.Store));
            Assert.True(((EncryptColumnCommand)result[5]).EncryptionFlags.HasFlag(ColumnEncryptionFlags.Search));
            Assert.False(((EncryptColumnCommand)result[5]).EncryptionFlags.HasFlag(ColumnEncryptionFlags.Addition));
            Assert.False(((EncryptColumnCommand)result[5]).EncryptionFlags.HasFlag(ColumnEncryptionFlags.Multiplication));
            Assert.True(((DecryptColumnCommand)result[6]).StatusCheck);
            Assert.Empty(((RebalanceOpetreeCommand)result[7]).WithValues);
            Assert.Equal(2, ((RebalanceOpetreeCommand)result[8]).WithValues.Count);
            Assert.IsType<SaveOpetreeCommand>(result[9]);
            Assert.IsType<LoadOpetreeCommand>(result[10]);
            Assert.IsType<LoadSchemaCommand>(result[11]);
            Assert.Equal(new TableRef("tt"), ((SelectQuery)((BypassCommand)result[12]).Query).GetTables()[0]);
            Assert.IsType<RefreshLicenseCommand>(result[13]);
            Assert.Equal("abc", ((SetLicenseKeyCommand)result[14]).LicenseKey.strvalue);
            Assert.IsType<CheckLicenseStatusCommand>(result[15]);
        }

        [Fact(DisplayName = "Parse SELECT w\\functions")]
        public void Parse_Function()
        {
            // Setup
            var test = "SELECT CONNECTION_ID()";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (SelectQuery)result[0];

            Assert.Equal(new Identifier("CONNECTION_ID"), ((ScalarFunction)actual.SelectExpressions[0]).FunctionName);
            Assert.Equal(new Identifier("CONNECTION_ID()"), ((ScalarFunction)actual.SelectExpressions[0]).Alias);
            Assert.Empty(((ScalarFunction)actual.SelectExpressions[0]).Parameters);
        }

        [Fact(DisplayName = "Parse SELECT w\\functions w\\params")]
        public void Parse_FunctionWithParams()
        {
            // Setup
            var test = "SELECT COUNT(tt.col1) AS Num, TEST('string',12)";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

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
            Assert.Equal(new ConstantContainer("string"), ((ScalarFunction)actual.SelectExpressions[1]).Parameters[0]);
            Assert.Equal(new ConstantContainer(12), ((ScalarFunction)actual.SelectExpressions[1]).Parameters[1]);
        }

        [Fact(DisplayName = "Parse INSERT INTO")]
        public void Parse_InsertInto()
        {
            // Setup
            var test =
                "INSERT INTO `tt1` (col1, col2, `col3`, `col4`) " +
                "VALUES ( -1, 12.345 , 'hey', \"hi\" ), " +
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
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (InsertQuery)result[0];

            Assert.Equal(new TableRef("tt1"), actual.Into);
            Assert.Equal(new Identifier("col1"), actual.Columns[0].ColumnName);
            Assert.Equal(new Identifier("col2"), actual.Columns[1].ColumnName);
            Assert.Equal(new Identifier("col3"), actual.Columns[2].ColumnName);
            Assert.Equal(new Identifier("col4"), actual.Columns[3].ColumnName);
            Assert.Equal(3, actual.Values.Count);
            Assert.Equal(new ConstantContainer(-1), actual.Values[0][0]);
            Assert.Equal(new ConstantContainer(12.345m), actual.Values[0][1]);
            Assert.Equal(new ConstantContainer(50), actual.Values[1][1]);
            Assert.Equal(new ConstantContainer((long)3147483647), actual.Values[1][2]);
            Assert.Equal(new ConstantContainer("  "), actual.Values[1][3]);
            Assert.Equal(new ConstantContainer("&"), actual.Values[1][4]);
            Assert.Equal(typeof(BinaryConstant), ((ConstantContainer)actual.Values[2][0]).constant.GetType());
            Assert.Equal(new ConstantContainer(new byte[] { 0x42, 0x02 }), actual.Values[2][1]);
            Assert.Equal(new ConstantContainer(new byte[] { 0xff, 0xff }), actual.Values[2][2]);
        }

        [Fact(DisplayName = "Parse SELECT")]
        public void Parse_Select()
        {
            // Setup
            var test = "SELECT (a+b)*(a+b), ((a+b)*(a+b)), (((a+b)*(a+b))) FROM t WHERE (a <= b) AND (t.b > a) AND c IN ('abc', 'def') AND d NOT IN (123, 456) GROUP BY t.a, b ORDER BY a ASC, b DESC, c";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (SelectQuery)result[0];

            Assert.Equal("(a+b)*(a+b)", actual.SelectExpressions[0].Alias.id);
            Assert.Equal("((a+b)*(a+b))", actual.SelectExpressions[1].Alias.id);
            Assert.Equal("(((a+b)*(a+b)))", actual.SelectExpressions[2].Alias.id);

            Assert.Equal(new ColumnRef("a"), ((BooleanLessThanEquals)actual.Where.CNF.AND[0].OR[0]).left);
            Assert.Equal(new ColumnRef("b"), ((BooleanLessThanEquals)actual.Where.CNF.AND[0].OR[0]).right);
            Assert.False(((BooleanLessThanEquals)actual.Where.CNF.AND[0].OR[0]).NOT);
            Assert.Equal(new ColumnRef("t", "b"), ((BooleanGreaterThan)actual.Where.CNF.AND[1].OR[0]).left);
            Assert.Equal(new ColumnRef("a"), ((BooleanGreaterThan)actual.Where.CNF.AND[1].OR[0]).right);
            Assert.False(((BooleanGreaterThan)actual.Where.CNF.AND[1].OR[0]).NOT);
            Assert.Equal(new ColumnRef("c"), ((BooleanIn)actual.Where.CNF.AND[2].OR[0]).Column);
            Assert.Equal(new ConstantContainer("abc"), ((BooleanIn)actual.Where.CNF.AND[2].OR[0]).InValues[0]);
            Assert.Equal(new ConstantContainer("def"), ((BooleanIn)actual.Where.CNF.AND[2].OR[0]).InValues[1]);
            Assert.False(((BooleanIn)actual.Where.CNF.AND[2].OR[0]).NOT);
            Assert.Equal(new ColumnRef("d"), ((BooleanIn)actual.Where.CNF.AND[3].OR[0]).Column);
            Assert.Equal(new ConstantContainer(123), ((BooleanIn)actual.Where.CNF.AND[3].OR[0]).InValues[0]);
            Assert.Equal(new ConstantContainer(456), ((BooleanIn)actual.Where.CNF.AND[3].OR[0]).InValues[1]);
            Assert.True(((BooleanIn)actual.Where.CNF.AND[3].OR[0]).NOT);

            Assert.Equal(new ColumnRef("t", "a"), actual.GroupBy.GetColumns()[0]);
            Assert.Equal(new ColumnRef("b"), actual.GroupBy.GetColumns()[1]);

            Assert.Equal(new Identifier("a"), actual.OrderBy.OrderColumns[0].Item1.ColumnName);
            Assert.Equal(new Identifier("b"), actual.OrderBy.OrderColumns[1].Item1.ColumnName);
            Assert.Equal(new Identifier("c"), actual.OrderBy.OrderColumns[2].Item1.ColumnName);
            Assert.Equal(OrderDirection.ASC, actual.OrderBy.OrderColumns[0].Item2);
            Assert.Equal(OrderDirection.DESC, actual.OrderBy.OrderColumns[1].Item2);
            Assert.Equal(OrderDirection.ASC, actual.OrderBy.OrderColumns[2].Item2);
        }

        [Fact(DisplayName = "Parse USE")]
        public void Parse_Use()
        {
            // Setup
            var test = "USE ThisDB";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (UseStatement)result[0];
            Assert.Equal(new DatabaseRef("ThisDB"), actual.Database);
        }

        [Fact(DisplayName = "Parse DROP TABLE")]
        public void Parse_DropTable()
        {
            // Setup
            var test = "DROP TABLE tt";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            var actual = (DropTableQuery)result[0];
            Assert.Equal(new TableRef("tt"), actual.TableName);
        }

        [Fact(DisplayName = "Parse variables")]
        public void Parse_Variables()
        {
            // Setup
            var test = "select @@version_comment limit 1; " +
                       "select @@`version_comment`";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            {
                var actual = (SelectQuery)result[0];
                Assert.Equal(new MySqlVariable("version_comment", "@@version_comment"), actual.SelectExpressions[0]);
                Assert.Equal((uint)1, actual.Limit);
            }
            {
                var actual = (SelectQuery)result[1];
                Assert.Equal(new MySqlVariable("version_comment", "@@`version_comment`"), actual.SelectExpressions[0]);
                Assert.Null(actual.Limit);
            }
        }

        [Fact(DisplayName = "Parse JOIN")]
        public void Parse_Join()
        {
            // Setup
            var test = "select tt1.a AS abc, tt2.b FROM tt1 AS table1 INNER JOIN tt2 ON table1.c=tt2.c; " +
                       "select tt1.a, tt2.b FROM tt1 JOIN tt2 ON tt1.c=tt2.c WHERE tt1.a=123; " +
                       "select tt1.a, tt2.b FROM tt1 CROSS JOIN tt2 LEFT OUTER JOIN tt3 ON tt3.c=tt2.c;";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            {
                var actual = (SelectQuery)result[0];
                Assert.Equal(new ColumnRef("tt1", "a", "abc"), actual.SelectExpressions[0]);
                Assert.Equal(new ColumnRef("tt2", "b"), actual.SelectExpressions[1]);
                Assert.Equal(new TableRef("tt1", AliasName: "table1"), ((TableSource)actual.From.Sources[0].FirstTable).Table);
                Assert.Equal(new TableRef("tt2"), ((TableSource)actual.From.Sources[0].JoinedTables[0].SecondTable).Table);
                Assert.Equal(new ColumnRef("table1", "c"), actual.From.Sources[0].JoinedTables[0].FirstColumn);
                Assert.Equal(new ColumnRef("tt2", "c"), actual.From.Sources[0].JoinedTables[0].SecondColumn);
                Assert.Equal(JoinType.INNER, actual.From.Sources[0].JoinedTables[0].JoinType);
            }
            {
                var actual = (SelectQuery)result[1];
                Assert.Equal(new ColumnRef("tt1", "a"), actual.SelectExpressions[0]);
                Assert.Equal(new ColumnRef("tt2", "b"), actual.SelectExpressions[1]);
                Assert.Equal(new TableRef("tt1"), ((TableSource)actual.From.Sources[0].FirstTable).Table);
                Assert.Equal(new TableRef("tt2"), ((TableSource)actual.From.Sources[0].JoinedTables[0].SecondTable).Table);
                Assert.Equal(new ColumnRef("tt1", "c"), actual.From.Sources[0].JoinedTables[0].FirstColumn);
                Assert.Equal(new ColumnRef("tt2", "c"), actual.From.Sources[0].JoinedTables[0].SecondColumn);
                Assert.Equal(new ColumnRef("tt1", "a"), ((BooleanEquals)actual.Where.CNF.AND[0].OR[0]).left);
                Assert.Equal(new ConstantContainer(123), ((BooleanEquals)actual.Where.CNF.AND[0].OR[0]).right);
                Assert.Equal(JoinType.INNER, actual.From.Sources[0].JoinedTables[0].JoinType);
            }
            {
                var actual = (SelectQuery)result[2];
                Assert.Equal(new ColumnRef("tt1", "a"), actual.SelectExpressions[0]);
                Assert.Equal(new ColumnRef("tt2", "b"), actual.SelectExpressions[1]);
                Assert.Equal(new TableRef("tt1"), ((TableSource)actual.From.Sources[0].FirstTable).Table);
                Assert.Equal(new TableRef("tt2"), ((TableSource)actual.From.Sources[0].JoinedTables[0].SecondTable).Table);
                Assert.Empty(actual.From.Sources[0].JoinedTables[0].GetColumns());
                Assert.Equal(JoinType.CROSS, actual.From.Sources[0].JoinedTables[0].JoinType);
                Assert.Equal(new TableRef("tt3"), ((TableSource)actual.From.Sources[0].JoinedTables[1].SecondTable).Table);
                Assert.Equal(new ColumnRef("tt3", "c"), actual.From.Sources[0].JoinedTables[1].FirstColumn);
                Assert.Equal(new ColumnRef("tt2", "c"), actual.From.Sources[0].JoinedTables[1].SecondColumn);
                Assert.Equal(2, actual.From.Sources[0].JoinedTables[1].GetColumns().Count);
                Assert.Equal(JoinType.LEFT_OUTER, actual.From.Sources[0].JoinedTables[1].JoinType);
            }
        }

        [Fact(DisplayName = "Parse All Columns")]
        public void Parse_AllColumns()
        {
            // Setup
            var test = "select * from tt; " +
                       "select t1.* from t1; ";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

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

        [Fact(DisplayName = "Parse known functions")]
        public void Parse_KnownFuncs()
        {
            // Setup
            var test = "SELECT RandomFunc(), SuM(col1), CoUNt(col2), coUNT(*), avg (col3), NOW(), STDDEV_SAMP(col4)";

            // Act
            var result = MySqlQueryParser.ParseToAst(test)[0] as SelectQuery;

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

            Assert.IsType<ScalarFunction>(result.SelectExpressions[5]);

            Assert.IsType<StDevAggregationFunction>(result.SelectExpressions[6]);
            Assert.IsType<ColumnRef>((result.SelectExpressions[6] as ScalarFunction).Parameters[0]);
        }

        [Fact(DisplayName = "Parse UPDATE")]
        public void Parse_Update()
        {
            // Setup
            var test = "UPDATE tt SET a = NULL WHERE b = 'abc'; ";

            // Act
            var result = MySqlQueryParser.ParseToAst(test)[0] as UpdateQuery;

            // Assert
            Assert.IsType<ColumnRef>(result.UpdateExpressions[0].Item1);
            Assert.IsType<NullConstant>(result.UpdateExpressions[0].Item2.constant);
        }

        [Fact(DisplayName = "Parse operators")]
        public void Parse_Operators()
        {
            // Setup
            var test = @"SELECT a+b, a-b, a*b, a/b
                         FROM   numerictable";

            // Act
            var result = MySqlQueryParser.ParseToAst(test)[0] as SelectQuery;

            // Assert
            Assert.Equal(4, result.SelectExpressions.Count);
            Assert.IsType<Addition>(result.SelectExpressions[0]);
            Assert.IsType<Subtraction>(result.SelectExpressions[1]);
            Assert.IsType<Multiplication>(result.SelectExpressions[2]);
            Assert.IsType<Division>(result.SelectExpressions[3]);
        }

        [Fact(DisplayName = "Parse LIKE")]
        public void Parse_Like()
        {
            // Setup
            var test = "SELECT * FROM TT WHERE a LIKE 'abc%'; " +
                       "SELECT * FROM TT WHERE a NOT LIKE 'a_34'; " +
                       "SELECT * FROM TT WHERE a LIKE 'abc%' ESCAPE '!'; ";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            Assert.Equal("a", (((BooleanLike)((SelectQuery)result[0]).Where.CNF.AND[0].OR[0]).Column.ColumnName.id));
            Assert.False(((BooleanLike)((SelectQuery)result[0]).Where.CNF.AND[0].OR[0]).NOT);
            Assert.Equal(new ConstantContainer("abc%"), ((BooleanLike)((SelectQuery)result[0]).Where.CNF.AND[0].OR[0]).SearchValue);
            Assert.Null(((BooleanLike)((SelectQuery)result[0]).Where.CNF.AND[0].OR[0]).EscapeChar);

            Assert.True(((BooleanLike)((SelectQuery)result[1]).Where.CNF.AND[0].OR[0]).NOT);
            Assert.Equal(new ConstantContainer("a_34"), ((BooleanLike)((SelectQuery)result[1]).Where.CNF.AND[0].OR[0]).SearchValue);
            Assert.Null(((BooleanLike)((SelectQuery)result[1]).Where.CNF.AND[0].OR[0]).EscapeChar);

            Assert.Equal('!', (((BooleanLike)((SelectQuery)result[2]).Where.CNF.AND[0].OR[0]).EscapeChar));
        }

        [Fact(DisplayName = "Parse SHOW")]
        public void Parse_Show()
        {
            // Setup
            var test = "SHOW TABLES;" +
                       "SHOW COLUMNS FROM abc;";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            Assert.True(result[0] is ShowTablesQuery);

            Assert.Equal(new TableRef("abc"), ((ShowColumnsQuery)result[1]).TableName);
        }

        [Fact(DisplayName = "Parse escaped")]
        public void Parse_Escaped()
        {
            // Setup 1
            // % and _ are automatically escaped outside of pattern-matching context; meaning that any \ before a % and _ is treated literally
            var test1 = "INSERT INTO TestTable (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (" +
                        "'ascii_null_\\0 foo\\0foo'," +
                        "'single_quote_\\' '''," +
                        "'double_quote1_\" \\\"'," +
                        "\"double_quote2_\\\" \"\"\"," +
                        "'backspace_\\b'," +
                        "'newline_\\n'," +
                        "'carriage_return_\\r'," +
                        "'tab_\\t'," +
                        "'ascii_26_\\Z'," +
                        "'backslash_\\\\'," +
                        "'percentage_% \\%'," +
                        "'underscore_ \\_'" +
                        ")";

            // Act 1
            var result1 = MySqlQueryParser.ParseToAst(test1);

            // Assert 1
            Assert.Equal(new ConstantContainer("ascii_null_\0 foo\0foo"), ((InsertQuery)result1[0]).Values[0][0]);
            Assert.Equal(new ConstantContainer("single_quote_' '"), ((InsertQuery)result1[0]).Values[0][1]);
            Assert.Equal(new ConstantContainer("double_quote1_\" \""), ((InsertQuery)result1[0]).Values[0][2]); // sql: \"
            Assert.Equal(new ConstantContainer("double_quote2_\" \""), ((InsertQuery)result1[0]).Values[0][3]);
            Assert.Equal(new ConstantContainer("backspace_\b"), ((InsertQuery)result1[0]).Values[0][4]);
            Assert.Equal(new ConstantContainer("newline_\n"), ((InsertQuery)result1[0]).Values[0][5]);
            Assert.Equal(new ConstantContainer("carriage_return_\r"), ((InsertQuery)result1[0]).Values[0][6]);
            Assert.Equal(new ConstantContainer("tab_\t"), ((InsertQuery)result1[0]).Values[0][7]);
            Assert.Equal(new ConstantContainer("ascii_26_\x1A"), ((InsertQuery)result1[0]).Values[0][8]);
            Assert.Equal(new ConstantContainer("backslash_\\"), ((InsertQuery)result1[0]).Values[0][9]);
            Assert.Equal(new ConstantContainer("percentage_% \\%"), ((InsertQuery)result1[0]).Values[0][10]);
            Assert.Equal(new ConstantContainer("underscore_ \\_"), ((InsertQuery)result1[0]).Values[0][11]);

            // Setup 2
            var test2 = "INSERT INTO TT1 (a, b, c, d, e) VALUES " +
                        "('I\\'m', '\\\"escaped\"', \"\"\"and\", '''me ''too')";
            // Act 2
            var result2 = MySqlQueryParser.ParseToAst(test2);

            // Assert 2
            Assert.Equal(new ConstantContainer("I'm"), ((InsertQuery)result2[0]).Values[0][0]);
            Assert.Equal(new ConstantContainer("\"escaped\""), ((InsertQuery)result2[0]).Values[0][1]);
            Assert.Equal(new ConstantContainer("\"and"), ((InsertQuery)result2[0]).Values[0][2]);
            Assert.Equal(new ConstantContainer("'me 'too"), ((InsertQuery)result2[0]).Values[0][3]);

            // Setup 3
            // use wxy as most unambigious character range without escape character usages. the rest are used as SQL, C# or ASCII escape chars
            var test3 = "INSERT INTO TestTable(a,b,c,d) VALUES (" +
                        "'mixed_\\b\\0\\t\\r\\n\\\n \\\\w \\x \\y'," +
                        "'order_of_replacement_1_\\ \\n \\\n \\\\n \\\\\n \\\\\\n \\\\\\\n \\\\\\\\n'," +
                        "'order_of_replacement_2_\\\r \\\t \\\\\\n \\\\\\\\\\\\\\\\\\b \\\\n\\\\r'," +
                        "'all_single_enclosed_\\0 \\\\\\\\ \\' '' \\t \\\" \\\\Z \\Z \\r\\n\\b \\\'\'\''," +
                        "\"all_double_enclosed_\\0 \\\\\\\\ \\' \\t \\\\\"\" \\\\Z \\Z \"\"\"\" \\r\\n\\b \\\"\"\"\"" +
                        ")";

            // Act 3
            var result3 = MySqlQueryParser.ParseToAst(test3);

            // Assert 3
            Assert.Equal(new ConstantContainer("mixed_\b\0\t\r\n\n \\w x y"), ((InsertQuery)result3[0]).Values[0][0]);
            Assert.Equal(new ConstantContainer("order_of_replacement_1_ \n \n \\n \\\n \\\n \\\n \\\\n"), ((InsertQuery)result3[0]).Values[0][1]);
            Assert.Equal(new ConstantContainer("order_of_replacement_2_\r \t \\\n \\\\\\\\\b \\n\\r"), ((InsertQuery)result3[0]).Values[0][2]);
            Assert.Equal(new ConstantContainer("all_single_enclosed_\0 \\\\ \' \' \t \" \\Z \x1A \r\n\b \'\'"), ((InsertQuery)result3[0]).Values[0][3]);
            Assert.Equal(new ConstantContainer("all_double_enclosed_\0 \\\\ \' \t \\\" \\Z \x1A \"\" \r\n\b \"\""), ((InsertQuery)result3[0]).Values[0][4]);
        }

        [Fact(DisplayName = "Parse order of operations")]
        public void Parse_OrderOfOperations()
        {
            // Setup
            var test = "SELECT ( a + b ) + ( c + d ) ;" +
                       "SELECT ( ( a + b ) + ( c + d ) ) ;" +
                       "SELECT a + ( b + c ) ;" +
                       "SELECT ( a + b ) + c ;" +
                       "SELECT a + b + c ;" +
                       "SELECT a * b + c ;" +
                       "SELECT a + b * c ; " +
                       "SELECT ( a + b ) * c ; " +
                       "SELECT ( a + b ) * ( c - d ) ; " +
                       "SELECT ( ( ( a + ( ( b * c ) ) ) / d ) ); ";

            // Act
            var result = MySqlQueryParser.ParseToAst(test);

            // Assert
            Assert.Equal("a", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[0]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[0]).SelectExpressions[0]).left).right).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[0]).SelectExpressions[0]).right).left).ColumnName.id);
            Assert.Equal("d", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[0]).SelectExpressions[0]).right).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[1]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[1]).SelectExpressions[0]).left).right).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[1]).SelectExpressions[0]).right).left).ColumnName.id);
            Assert.Equal("d", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[1]).SelectExpressions[0]).right).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((SelectQuery)result[2]).SelectExpressions[0]).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[2]).SelectExpressions[0]).right).left).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[2]).SelectExpressions[0]).right).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[3]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[3]).SelectExpressions[0]).left).right).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Addition)((SelectQuery)result[3]).SelectExpressions[0]).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[4]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Addition)((Addition)((SelectQuery)result[4]).SelectExpressions[0]).left).right).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Addition)((SelectQuery)result[4]).SelectExpressions[0]).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Multiplication)((Addition)((SelectQuery)result[5]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Multiplication)((Addition)((SelectQuery)result[5]).SelectExpressions[0]).left).right).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Addition)((SelectQuery)result[5]).SelectExpressions[0]).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((SelectQuery)result[6]).SelectExpressions[0]).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Multiplication)((Addition)((SelectQuery)result[6]).SelectExpressions[0]).right).left).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Multiplication)((Addition)((SelectQuery)result[6]).SelectExpressions[0]).right).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((Multiplication)((SelectQuery)result[7]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Addition)((Multiplication)((SelectQuery)result[7]).SelectExpressions[0]).left).right).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Multiplication)((SelectQuery)result[7]).SelectExpressions[0]).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((Multiplication)((SelectQuery)result[8]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Addition)((Multiplication)((SelectQuery)result[8]).SelectExpressions[0]).left).right).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Subtraction)((Multiplication)((SelectQuery)result[8]).SelectExpressions[0]).right).left).ColumnName.id);
            Assert.Equal("d", ((ColumnRef)((Subtraction)((Multiplication)((SelectQuery)result[8]).SelectExpressions[0]).right).right).ColumnName.id);

            Assert.Equal("a", ((ColumnRef)((Addition)((Division)((SelectQuery)result[9]).SelectExpressions[0]).left).left).ColumnName.id);
            Assert.Equal("b", ((ColumnRef)((Multiplication)((Addition)((Division)((SelectQuery)result[9]).SelectExpressions[0]).left).right).left).ColumnName.id);
            Assert.Equal("c", ((ColumnRef)((Multiplication)((Addition)((Division)((SelectQuery)result[9]).SelectExpressions[0]).left).right).right).ColumnName.id);
            Assert.Equal("d", ((ColumnRef)((Division)((SelectQuery)result[9]).SelectExpressions[0]).right).ColumnName.id);
        }

        [Fact(DisplayName = "Parse SELECT sub query")]
        public void Parse_SelectSubQuery()
        {
            // Setup
            var test = "SELECT a, b, c FROM (SELECT z FROM tt) AS t;" +
                       "SELECT a, b, c FROM ss, (SELECT z FROM tt) AS t, uu as u;";

            // Act
            var results = MySqlQueryParser.ParseToAst(test);

            var result1 = results[0] as SelectQuery;
            Assert.Single(result1.From.Sources);
            Assert.Equal("t", ((SelectSubQuery)result1.From.Sources[0].FirstTable).Alias.id);
            Assert.Equal("tt", ((TableSource)((SelectSubQuery)result1.From.Sources[0].FirstTable).Select.From.Sources[0].FirstTable).Table.Table.id);
            Assert.Single(((SelectSubQuery)result1.From.Sources[0].FirstTable).Select.SelectExpressions);

            var result2 = results[1] as SelectQuery;
            Assert.Equal(3, result2.From.Sources.Count);
            Assert.Equal("t", ((SelectSubQuery)result2.From.Sources[1].FirstTable).Alias.id);
            Assert.Equal("tt", ((TableSource)((SelectSubQuery)result2.From.Sources[1].FirstTable).Select.From.Sources[0].FirstTable).Table.Table.id);
            Assert.Single(((SelectSubQuery)result2.From.Sources[1].FirstTable).Select.SelectExpressions);
            Assert.Equal("ss", ((TableSource)result2.From.Sources[0].FirstTable).Table.Table.id);
            Assert.Equal("uu", ((TableSource)result2.From.Sources[2].FirstTable).Table.Table.id);
            Assert.Equal("u", ((TableSource)result2.From.Sources[2].FirstTable).Table.Alias.id);
        }
    }
}
