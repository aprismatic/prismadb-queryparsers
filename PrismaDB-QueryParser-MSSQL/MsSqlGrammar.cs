using System;
using Irony.Parsing;

namespace PrismaDB.QueryParser.MSSQL
{
    // Reference: https://github.com/IronyProject/Irony/tree/master/Irony.Samples/SQL
    [Language("Prisma/DB MSSQL", "0.1", "Extended MSSQL grammer for Prisma/DB")]
    public class MsSqlGrammar : Grammar
    {
        public MsSqlGrammar() : base(false)
        {
            // SQL is case insensitive
            // Terminals
            var comment = new CommentTerminal("comment", "/*", "*/");
            var lineComment = new CommentTerminal("line_comment", "--", "\n", "\r\n");
            NonGrammarTerminals.Add(comment);
            NonGrammarTerminals.Add(lineComment);
            var number = new NumberLiteral("number", NumberOptions.AllowSign)
            {
                DefaultIntTypes = new[] { NumberLiteral.TypeCodeBigInt },
                DefaultFloatType = TypeCode.Decimal,
                DecimalSeparator = '.'
            };
            number.AddPrefix("0x", NumberOptions.Hex);
            var string_literal = new StringLiteral("string", "'", StringOptions.AllowsDoubledQuote);
            // Normal identifiers (abc) and quoted id's ([abc d], "abc d")
            var Id_simple = TerminalFactory.CreateSqlExtIdentifier(this, "id_simple");
            Id_simple.AllFirstChars += "*";
            var comma = ToTerm(",");
            var dot = ToTerm(".");
            var CREATE = ToTerm("CREATE");
            var NULL = ToTerm("NULL");
            var NOT = ToTerm("NOT");
            var TABLE = ToTerm("TABLE");
            var ALTER = ToTerm("ALTER");
            var COLUMN = ToTerm("COLUMN");
            var ON = ToTerm("ON");
            var JOIN = ToTerm("JOIN");
            var INNER = ToTerm("INNER");
            var OUTER = ToTerm("OUTER");
            var LEFT = ToTerm("LEFT");
            var RIGHT = ToTerm("RIGHT");
            var FULL = ToTerm("FULL");
            var CROSS = ToTerm("CROSS");
            var INSERT = ToTerm("INSERT");
            var INTO = ToTerm("INTO");
            var UPDATE = ToTerm("UPDATE");
            var SET = ToTerm("SET");
            var VALUES = ToTerm("VALUES");
            var DELETE = ToTerm("DELETE");
            var SELECT = ToTerm("SELECT");
            var FROM = ToTerm("FROM");
            var AS = ToTerm("AS");
            var BY = ToTerm("BY");
            var ENCRYPTED = ToTerm("ENCRYPTED");
            var FOR = ToTerm("FOR");
            var USE = ToTerm("USE");
            var TOP = ToTerm("TOP");
            var DEFAULT = ToTerm("DEFAULT");
            var CURRENT_TIMESTAMP = ToTerm("CURRENT_TIMESTAMP");
            var PRISMADB = ToTerm("PRISMADB");
            var TO = ToTerm("TO");
            var STAR = ToTerm("*");
            var IDENTITY = ToTerm("IDENTITY");

            // Non-Terminals
            var Id = new NonTerminal("Id");
            var stmt = new NonTerminal("stmt");
            var createTableStmt = new NonTerminal("createTableStmt");
            var alterStmt = new NonTerminal("alterStmt");
            var selectStmt = new NonTerminal("selectStmt");
            var insertStmt = new NonTerminal("insertStmt");
            var updateStmt = new NonTerminal("updateStmt");
            var deleteStmt = new NonTerminal("deleteStmt");
            var useStmt = new NonTerminal("useStmt");

            var exportSettingsCmd = new NonTerminal("exportSettingsCmd");

            var fieldDef = new NonTerminal("fieldDef");
            var fieldDefList = new NonTerminal("fieldDefList");
            var nullSpecOpt = new NonTerminal("nullSpecOpt");
            var typeName = new NonTerminal("typeName");
            var typeParamsOpt = new NonTerminal("typeParams");
            var idlist = new NonTerminal("idlist");
            var idlistPar = new NonTerminal("idlistPar");
            var orderList = new NonTerminal("orderList");
            var orderMember = new NonTerminal("orderMember");
            var orderDirOpt = new NonTerminal("orderDirOpt");
            var alterCmd = new NonTerminal("alterCmd");
            var insertData = new NonTerminal("insertData");
            var insertDataList = new NonTerminal("insertDataList");
            var intoOpt = new NonTerminal("intoOpt");
            var assignList = new NonTerminal("assignList");
            var whereClauseOpt = new NonTerminal("whereClauseOpt");
            var groupClauseOpt = new NonTerminal("groupClauseOpt");
            var assignment = new NonTerminal("assignment");
            var expression = new NonTerminal("expression");
            var exprList = new NonTerminal("exprList");
            var selRestrOpt = new NonTerminal("selRestrOpt");
            var selList = new NonTerminal("selList");
            var fromClauseOpt = new NonTerminal("fromClauseOpt");
            var orderClauseOpt = new NonTerminal("orderClauseOpt");
            var columnItemList = new NonTerminal("columnItemList");
            var columnItem = new NonTerminal("columnItem");
            var columnSource = new NonTerminal("columnSource");
            var asOpt = new NonTerminal("asOpt");
            var aliasOpt = new NonTerminal("aliasOpt");
            var tuple = new NonTerminal("tuple");
            var joinClauseListOpt = new NonTerminal("joinClauseListOpt");
            var joinClauseList = new NonTerminal("joinClauseList");
            var joinClause = new NonTerminal("joinClause");
            var joinKindOpt = new NonTerminal("joinKindOpt");
            var joinOnOpt = new NonTerminal("joinOnOpt");
            var term = new NonTerminal("term");
            var unExpr = new NonTerminal("unExpr");
            var unOp = new NonTerminal("unOp");
            var binExpr = new NonTerminal("binExpr");
            var binOp = new NonTerminal("binOp");
            var notOpt = new NonTerminal("notOpt");
            var funCall = new NonTerminal("funCall");
            var stmtLine = new NonTerminal("stmtLine");
            var semiOpt = new NonTerminal("semiOpt");
            var stmtList = new NonTerminal("stmtList");
            var funArgs = new NonTerminal("funArgs");
            var autoDefaultOpt = new NonTerminal("autoDefaultOpt");

            var encryptionOpt = new NonTerminal("encryptionOpt");
            var encryptTypePar = new NonTerminal("encryptTypePar");
            var encryptTypeList = new NonTerminal("encryptTypeList");
            var encryptType = new NonTerminal("encryptType");

            // BNF Rules
            Root = stmtList;
            stmtLine.Rule = stmt + semiOpt;
            semiOpt.Rule = Empty | ";";
            stmtList.Rule = MakePlusRule(stmtList, stmtLine);

            // ID
            Id.Rule = MakePlusRule(Id, dot, Id_simple);

            stmt.Rule = createTableStmt | alterStmt | selectStmt | insertStmt | updateStmt | deleteStmt | useStmt |
                        exportSettingsCmd |
                        "GO";

            // Create Statement
            createTableStmt.Rule = CREATE + TABLE + Id + "(" + fieldDefList + ")";
            fieldDefList.Rule = MakePlusRule(fieldDefList, comma, fieldDef);
            fieldDef.Rule = Id + typeName + typeParamsOpt + encryptionOpt + nullSpecOpt + autoDefaultOpt;

            var t_INT = ToTerm("INT");
            var t_SMALLINT = ToTerm("SMALLINT");
            var t_TINYINT = ToTerm("TINYINT");
            var t_BIGINT = ToTerm("BIGINT");
            var t_CHAR = ToTerm("CHAR");
            var t_VARCHAR = ToTerm("VARCHAR");
            var t_NCHAR = ToTerm("NCHAR");
            var t_NVARCHAR = ToTerm("NVARCHAR");
            var t_TEXT = ToTerm("TEXT");
            var t_NTEXT = ToTerm("NTEXT");
            var t_BINARY = ToTerm("BINARY");
            var t_VARBINARY = ToTerm("VARBINARY");
            var t_UNIQUEIDENTIFIER = ToTerm("UNIQUEIDENTIFIER");
            var t_DATETIME = ToTerm("DATETIME");
            var t_FLOAT = ToTerm("FLOAT");
            var t_DATE = ToTerm("DATE");
            var t_MAX = ToTerm("MAX");
            typeName.Rule = t_INT | t_CHAR | t_VARCHAR | t_NCHAR | t_NVARCHAR | t_TEXT | t_NTEXT | t_BINARY | t_VARBINARY |
                            t_UNIQUEIDENTIFIER | t_DATETIME | t_FLOAT | t_BIGINT | t_SMALLINT | t_TINYINT | t_DATE;
            typeParamsOpt.Rule = ("(" + number + ")") | ("(" + t_MAX + ")") | Empty;

            var et_STORE = ToTerm("STORE");
            var et_INTEGER_ADDITION = ToTerm("INTEGER_ADDITION");
            var et_INTEGER_MULTIPLICATION = ToTerm("INTEGER_MULTIPLICATION");
            var et_SEARCH = ToTerm("SEARCH");
            var et_RANGE = ToTerm("RANGE");
            encryptionOpt.Rule = (ENCRYPTED + encryptTypePar) | Empty;
            encryptTypePar.Rule = (FOR + "(" + encryptTypeList + ")") | Empty;
            encryptTypeList.Rule = MakePlusRule(encryptTypeList, comma, encryptType);
            encryptType.Rule = et_STORE | et_INTEGER_ADDITION | et_INTEGER_MULTIPLICATION | et_SEARCH | et_RANGE;

            nullSpecOpt.Rule = NULL | (NOT + NULL) | Empty;
            autoDefaultOpt.Rule = (DEFAULT + term) | (IDENTITY + "(" + "1" + comma + "1" + ")") | Empty;

            // Alter Statement
            alterStmt.Rule = ALTER + TABLE + Id + alterCmd;
            alterCmd.Rule = ALTER + COLUMN + fieldDef;

            // Command Statement
            exportSettingsCmd.Rule = PRISMADB + "EXPORT" + "SETTINGS" + TO + string_literal;

            // Use Statement
            useStmt.Rule = USE + Id;

            // Insert Statement
            insertStmt.Rule = INSERT + intoOpt + Id + idlistPar + VALUES + insertDataList;
            insertData.Rule = "(" + exprList + ")";
            intoOpt.Rule = Empty | INTO; // Into is optional in MSSQL
            idlistPar.Rule = "(" + idlist + ")";
            idlist.Rule = MakePlusRule(idlist, comma, Id);
            insertDataList.Rule = MakePlusRule(insertDataList, comma, insertData);

            // Update Statement
            updateStmt.Rule = UPDATE + Id + SET + assignList + whereClauseOpt;
            assignList.Rule = MakePlusRule(assignList, comma, assignment);
            assignment.Rule = Id + "=" + expression;

            // Delete Statement
            deleteStmt.Rule = DELETE + FROM + Id + whereClauseOpt;

            // Select Statement
            selectStmt.Rule = SELECT + selRestrOpt + selList + fromClauseOpt + whereClauseOpt + groupClauseOpt + orderClauseOpt;
            selRestrOpt.Rule = Empty | (TOP + number) | (TOP + "(" + number + ")");
            selList.Rule = columnItemList | STAR;
            columnItemList.Rule = MakePlusRule(columnItemList, comma, columnItem);
            columnItem.Rule = columnSource + aliasOpt;
            aliasOpt.Rule = Empty | (asOpt + Id);
            asOpt.Rule = Empty | AS;
            columnSource.Rule = expression;
            fromClauseOpt.Rule = Empty | FROM + idlist + joinClauseListOpt;
            joinClauseListOpt.Rule = Empty | joinClauseList;
            joinClauseList.Rule = MakePlusRule(joinClauseList, joinClause);
            joinClause.Rule = joinKindOpt + JOIN + Id + joinOnOpt;
            joinKindOpt.Rule = Empty | INNER | LEFT | LEFT + OUTER | RIGHT | RIGHT + OUTER | FULL | FULL + OUTER | CROSS;
            joinOnOpt.Rule = Empty | ON + Id + "=" + Id;
            whereClauseOpt.Rule = Empty | ("WHERE" + expression);
            groupClauseOpt.Rule = Empty | ("GROUP" + BY + idlist);
            orderClauseOpt.Rule = Empty | ("ORDER" + BY + orderList);
            orderList.Rule = MakePlusRule(orderList, comma, orderMember);
            orderMember.Rule = Id + orderDirOpt;
            orderDirOpt.Rule = Empty | "ASC" | "DESC";

            // Expression
            exprList.Rule = MakePlusRule(exprList, comma, expression);
            expression.Rule = term | unExpr | binExpr;
            term.Rule = Id | string_literal | number | tuple | funCall | NULL;
            tuple.Rule = "(" + exprList + ")";
            unExpr.Rule = unOp + term;
            unOp.Rule = NOT | "+" | "-" | "~";
            binExpr.Rule = (expression + binOp + expression) | ("(" + expression + binOp + expression + ")");
            binOp.Rule = ToTerm("+") | "-" | "*" | "/" | "%" // Arithmetic
                         | "&" | "|" | "^" // Bit
                         | "=" | ">" | "<" | ">=" | "<=" | "<>" | "!=" | "!<" | "!>"
                         | "AND" | "OR" | "LIKE" | (NOT + "LIKE") | "IN" | (NOT + "IN") | "IS" + notOpt;
            notOpt.Rule = Empty | NOT;
            funCall.Rule = (Id + "(" + funArgs + ")") | CURRENT_TIMESTAMP;
            funArgs.Rule = Empty | exprList | STAR;

            // Operators
            RegisterOperators(10, "*", "/", "%");
            RegisterOperators(9, "+", "-");
            RegisterOperators(8, "=", ">", "<", ">=", "<=", "<>", "!=", "!<", "!>", "LIKE", "IN", "IS");
            RegisterOperators(7, "^", "&", "|");
            RegisterOperators(6, NOT);
            RegisterOperators(5, "AND");
            RegisterOperators(4, "OR");

            MarkPunctuation(",", "(", ")");
            MarkPunctuation(asOpt, semiOpt);
            //Note: we cannot declare binOp as transient because it includes operators "NOT LIKE", "NOT IN" consisting of two tokens. 
            // Transient non-terminals cannot have more than one non-punctuation child nodes.
            // Instead, we set flag InheritPrecedence on binOp , so that it inherits precedence value from it's children, and this precedence is used
            // in conflict resolution when binOp node is sitting on the stack
            MarkTransient(stmt, term, asOpt, aliasOpt, stmtLine, expression, unOp, tuple);
            binOp.SetFlag(TermFlags.InheritPrecedence);
        }
    }
}