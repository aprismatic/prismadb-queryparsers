using System;
using System.Numerics;
using Irony.Parsing;
using PrismaDB.QueryAST.DDL;

namespace PrismaDB.QueryParser.MSSQL
{
    public partial class MsSqlParser
    {
        /// <summary>
        ///     Builds a Create Table Query.
        /// </summary>
        /// <param name="createQuery">Resulting CreateTableQuery object</param>
        /// <param name="node">Parent node of query</param>
        private static void BuildCreateTableQuery(CreateTableQuery createQuery, ParseTreeNode node)
        {
            foreach (var mainNode in node.ChildNodes)
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                    createQuery.TableName = BuildTableRef(mainNode);
                // Check for columns
                else if (mainNode.Term.Name.Equals("fieldDefList"))
                    foreach (var fieldDefNode in mainNode.ChildNodes)
                        createQuery.ColumnDefinitions.Add(BuildColumnDefinition(fieldDefNode));
        }


        /// <summary>
        ///     Builds a Alter Table Query.
        /// </summary>
        /// <param name="alterQuery">Resulting AlterTableQuery object</param>
        /// <param name="node">Parent node of query</param>
        private static void BuildAlterTableQuery(AlterTableQuery alterQuery, ParseTreeNode node)
        {
            foreach (var mainNode in node.ChildNodes)
                // Check for table name
                if (mainNode.Term.Name.Equals("Id"))
                {
                    alterQuery.TableName = BuildTableRef(mainNode);
                }
                // Check for column
                else if (mainNode.Term.Name.Equals("alterCmd"))
                {
                    // Only ALTER COLUMN is supported now
                    alterQuery.AlterType = AlterType.MODIFY;
                    alterQuery.AlteredColumns.Add(new AlteredColumn(
                        BuildColumnDefinition(
                            FindChildNode(mainNode, "fieldDef"))));
                }
        }


        /// <summary>
        ///     Builds Column Definition.
        /// </summary>
        /// <param name="node">Column Definition node</param>
        /// <returns>Resulting Column Definition</returns>
        private static ColumnDefinition BuildColumnDefinition(ParseTreeNode node)
        {
            // Create and set name of column definition
            var colDef = new ColumnDefinition(BuildColumnRef(FindChildNode(node, "Id")).ColumnName, SqlDataType.MSSQL_INT);

            // Check for datatype
            var dataTypeNode = FindChildNode(node, "typeName");

            var requiredLength = false;
            var prohibitedLength = false;

            if (FindChildNode(dataTypeNode, "INT") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_INT;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "TINYINT") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_TINYINT;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "SMALLINT") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_SMALLINT;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "BIGINT") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_BIGINT;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "FLOAT") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_FLOAT;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "DATE") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_DATE;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "DATETIME") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_DATETIME;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "CHAR") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_CHAR;
            }
            else if (FindChildNode(dataTypeNode, "VARCHAR") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_VARCHAR;
            }
            else if (FindChildNode(dataTypeNode, "TEXT") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_TEXT;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "NCHAR") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_NCHAR;
            }
            else if (FindChildNode(dataTypeNode, "NVARCHAR") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_NVARCHAR;
            }
            else if (FindChildNode(dataTypeNode, "NTEXT") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_NTEXT;
                prohibitedLength = true;
            }
            else if (FindChildNode(dataTypeNode, "BINARY") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_BINARY;
            }
            else if (FindChildNode(dataTypeNode, "VARBINARY") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_VARBINARY;
            }
            else if (FindChildNode(dataTypeNode, "UNIQUEIDENTIFIER") != null)
            {
                colDef.DataType = SqlDataType.MSSQL_UNIQUEIDENTIFIER;
                prohibitedLength = true;
            }

            // Check for datatype length
            var paraNode = FindChildNode(node, "typeParams");
            if (paraNode != null)
            {
                var numberNode = FindChildNode(paraNode, "number");
                if (numberNode != null)
                {
                    if (prohibitedLength)
                        throw new ApplicationException("Datatype cannot have length");

                    colDef.Length = (int)(BigInteger)numberNode.Token.Value;
                }
                else
                {
                    var maxNode = FindChildNode(paraNode, "MAX");
                    if (maxNode != null)
                        colDef.Length = -1;
                }
            }
            else
            {
                if (requiredLength)
                    throw new ApplicationException("Length is required");

                if (!prohibitedLength)
                    colDef.Length = 1;
            }

            // Check for nullable
            colDef.Nullable = CheckNull(FindChildNode(node, "nullSpecOpt"));

            // Check for encryption
            colDef.EncryptionFlags = CheckEncryption(FindChildNode(node, "encryptionOpt"));

            // Check for autoDefault value 
            var autoDefaultNode = FindChildNode(node, "autoDefaultOpt");
            if (FindChildNode(autoDefaultNode, "DEFAULT") != null)
                colDef.DefaultValue = BuildExpression(autoDefaultNode.ChildNodes[1]);
            else if (FindChildNode(autoDefaultNode, "IDENTITY") != null)
                colDef.AutoIncrement = true;
            return colDef;
        }


        /// <summary>
        ///     Check for encryption schemes.
        /// </summary>
        /// <param name="node">Parent node of query</param>
        /// <returns>Column encryption enum flags</returns>
        public static ColumnEncryptionFlags CheckEncryption(ParseTreeNode node)
        {
            if (node == null)
                return ColumnEncryptionFlags.None;

            var encryptTypeParNode = FindChildNode(node, "encryptTypePar");
            if (encryptTypeParNode == null && FindChildNode(node, "ENCRYPTED") == null)
                return ColumnEncryptionFlags.None;

            var encryptTypeNodes = FindChildNode(encryptTypeParNode, "encryptTypeList");
            if (encryptTypeNodes == null)
                return ColumnEncryptionFlags.Store;

            var flags = ColumnEncryptionFlags.None;
            foreach (var childNode in encryptTypeNodes.ChildNodes)
                if (FindChildNode(childNode, "STORE") != null)
                    flags |= ColumnEncryptionFlags.Store;
                else if (FindChildNode(childNode, "ADDITION") != null)
                    flags |= ColumnEncryptionFlags.Addition;
                else if (FindChildNode(childNode, "MULTIPLICATION") != null)
                    flags |= ColumnEncryptionFlags.Multiplication;
                else if (FindChildNode(childNode, "SEARCH") != null)
                    flags |= ColumnEncryptionFlags.Search;
                else if (FindChildNode(childNode, "RANGE") != null)
                    flags |= ColumnEncryptionFlags.Range;
            return flags;
        }


        /// <summary>
        ///     Check for NOT NULL.
        /// </summary>
        /// <param name="node">Parent node of query</param>
        /// <returns>True if nullable</returns>
        public static bool CheckNull(ParseTreeNode node)
        {
            if (node != null)
                if (node.ChildNodes.Count > 1)
                    if (node.ChildNodes[0].Token.ValueString.Equals("not") &&
                        node.ChildNodes[1].Token.ValueString.Equals("null"))
                        return false;
            return true;
        }
    }
}