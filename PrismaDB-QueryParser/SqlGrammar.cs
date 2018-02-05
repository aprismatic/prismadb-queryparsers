﻿using Irony.Parsing;

namespace PrismaDB.QueryParser
{
    // Loosely based on SQL89 grammar from Gold parser. Supports some extra TSQL constructs.

    [Language("SQL", "89", "SQL 89 grammar")]
    public class SqlGrammar : Grammar
    {
        public SqlGrammar() : base(false)
        { //SQL is case insensitive
          //Terminals
            var comment = new CommentTerminal("comment", "/*", "*/");
            var lineComment = new CommentTerminal("line_comment", "--", "\n", "\r\n");
            NonGrammarTerminals.Add(comment);
            NonGrammarTerminals.Add(lineComment);
            var number = new NumberLiteral("number");
            var string_literal = new StringLiteral("string", "'", StringOptions.AllowsDoubledQuote);
            var Id_simple = TerminalFactory.CreateSqlExtIdentifier(this, "id_simple"); //covers normal identifiers (abc) and quoted id's ([abc d], "abc d")
            var comma = ToTerm(",");
            var dot = ToTerm(".");
            var CREATE = ToTerm("CREATE");
            var NULL = ToTerm("NULL");
            var NOT = ToTerm("NOT");
            //var UNIQUE = ToTerm("UNIQUE"); 
            //var WITH = ToTerm("WITH");
            var TABLE = ToTerm("TABLE");
            var ALTER = ToTerm("ALTER");
            //var ADD = ToTerm("ADD"); 
            var COLUMN = ToTerm("COLUMN");
            //var DROP = ToTerm("DROP"); 
            //var CONSTRAINT = ToTerm("CONSTRAINT");
            //var INDEX = ToTerm("INDEX"); 
            //var ON = ToTerm("ON");
            //var KEY = ToTerm("KEY");
            //var PRIMARY = ToTerm("PRIMARY");
            var INSERT = ToTerm("INSERT");
            var INTO = ToTerm("INTO");
            var UPDATE = ToTerm("UPDATE");
            var SET = ToTerm("SET");
            var VALUES = ToTerm("VALUES");
            var DELETE = ToTerm("DELETE");
            var SELECT = ToTerm("SELECT");
            var FROM = ToTerm("FROM");
            var AS = ToTerm("AS");
            //var COUNT = ToTerm("COUNT");
            //var JOIN = ToTerm("JOIN");
            //var BY = ToTerm("BY");
            var ENCRYPTED = ToTerm("ENCRYPTED");
            var FOR = ToTerm("FOR");
            var USE = ToTerm("USE");
            var TOP = ToTerm("TOP");
            var IDENTITY = ToTerm("IDENTITY(1,1)");
            var DEFAULT = ToTerm("DEFAULT");

            //Non-terminals
            var Id = new NonTerminal("Id");
            var stmt = new NonTerminal("stmt");
            var createTableStmt = new NonTerminal("createTableStmt");
            //var createIndexStmt = new NonTerminal("createIndexStmt");
            var alterStmt = new NonTerminal("alterStmt");
            var dropTableStmt = new NonTerminal("dropTableStmt");
            var dropIndexStmt = new NonTerminal("dropIndexStmt");
            var selectStmt = new NonTerminal("selectStmt");
            var insertStmt = new NonTerminal("insertStmt");
            var updateStmt = new NonTerminal("updateStmt");
            var deleteStmt = new NonTerminal("deleteStmt");
            var useStmt = new NonTerminal("useStmt");
            var fieldDef = new NonTerminal("fieldDef");
            var fieldDefList = new NonTerminal("fieldDefList");
            var nullSpecOpt = new NonTerminal("nullSpecOpt");
            var typeName = new NonTerminal("typeName");
            var typeSpec = new NonTerminal("typeSpec");
            var typeParamsOpt = new NonTerminal("typeParams");
            var constraintDef = new NonTerminal("constraintDef");
            //var constraintListOpt = new NonTerminal("constraintListOpt");
            var constraintTypeOpt = new NonTerminal("constraintTypeOpt");
            var idlist = new NonTerminal("idlist");
            var idlistPar = new NonTerminal("idlistPar");
            var uniqueOpt = new NonTerminal("uniqueOpt");
            var orderList = new NonTerminal("orderList");
            var orderMember = new NonTerminal("orderMember");
            var orderDirOpt = new NonTerminal("orderDirOpt");
            var withClauseOpt = new NonTerminal("withClauseOpt");
            var alterCmd = new NonTerminal("alterCmd");
            var insertData = new NonTerminal("insertData");
            var intoOpt = new NonTerminal("intoOpt");
            var assignList = new NonTerminal("assignList");
            var whereClauseOpt = new NonTerminal("whereClauseOpt");
            var assignment = new NonTerminal("assignment");
            var expression = new NonTerminal("expression");
            var exprList = new NonTerminal("exprList");
            var selRestrOpt = new NonTerminal("selRestrOpt");
            var selList = new NonTerminal("selList");
            var intoClauseOpt = new NonTerminal("intoClauseOpt");
            var fromClauseOpt = new NonTerminal("fromClauseOpt");
            var groupClauseOpt = new NonTerminal("groupClauseOpt");
            var havingClauseOpt = new NonTerminal("havingClauseOpt");
            var orderClauseOpt = new NonTerminal("orderClauseOpt");
            var columnItemList = new NonTerminal("columnItemList");
            var columnItem = new NonTerminal("columnItem");
            var columnSource = new NonTerminal("columnSource");
            var asOpt = new NonTerminal("asOpt");
            var aliasOpt = new NonTerminal("aliasOpt");
            var aggregate = new NonTerminal("aggregate");
            var aggregateArg = new NonTerminal("aggregateArg");
            var aggregateName = new NonTerminal("aggregateName");
            var tuple = new NonTerminal("tuple");
            var joinChainOpt = new NonTerminal("joinChainOpt");
            var joinKindOpt = new NonTerminal("joinKindOpt");
            var term = new NonTerminal("term");
            var unExpr = new NonTerminal("unExpr");
            var unOp = new NonTerminal("unOp");
            var binExpr = new NonTerminal("binExpr");
            var binOp = new NonTerminal("binOp");
            var betweenExpr = new NonTerminal("betweenExpr");
            var inExpr = new NonTerminal("inExpr");
            //var parSelectStmt = new NonTerminal("parSelectStmt");
            var notOpt = new NonTerminal("notOpt");
            var funCall = new NonTerminal("funCall");
            var stmtLine = new NonTerminal("stmtLine");
            var semiOpt = new NonTerminal("semiOpt");
            var stmtList = new NonTerminal("stmtList");
            var funArgs = new NonTerminal("funArgs");
            var inStmt = new NonTerminal("inStmt");

            var insertDataList = new NonTerminal("insertDataList"); // new
            var autoDefaultOpt = new NonTerminal("autoDefaultOpt"); // new 

            var encryptionOpt = new NonTerminal("encryptionOpt");
            var encryptTypePar = new NonTerminal("encryptTypePar");
            var encryptTypeList = new NonTerminal("encryptTypeList");
            var encryptType = new NonTerminal("encryptType");

            //BNF Rules
            this.Root = stmtList;
            stmtLine.Rule = stmt + semiOpt;
            semiOpt.Rule = Empty | ";";
            stmtList.Rule = MakePlusRule(stmtList, stmtLine);

            //ID
            Id.Rule = MakePlusRule(Id, dot, Id_simple);

            stmt.Rule = createTableStmt | alterStmt //| createIndexStmt
                                                    //| dropTableStmt | dropIndexStmt
                      | selectStmt | insertStmt | updateStmt | deleteStmt | useStmt
                      | "GO";
            //Create table
            createTableStmt.Rule = CREATE + TABLE + Id + "(" + fieldDefList + ")"; //+ constraintListOpt;
            fieldDefList.Rule = MakePlusRule(fieldDefList, comma, fieldDef);
            fieldDef.Rule = Id + typeName + typeParamsOpt + encryptionOpt + nullSpecOpt + autoDefaultOpt;

            encryptionOpt.Rule = ENCRYPTED + encryptTypePar | Empty;
            encryptTypePar.Rule = FOR + "(" + encryptTypeList + ")" | Empty;
            encryptTypeList.Rule = MakePlusRule(encryptTypeList, comma, encryptType);

            var et_STORE = ToTerm("STORE");
            var et_INTEGER_ADDITION = ToTerm("INTEGER_ADDITION");
            var et_INTEGER_MULTIPLICATION = ToTerm("INTEGER_MULTIPLICATION");
            var et_SEARCH = ToTerm("SEARCH");

            encryptType.Rule = et_STORE | et_INTEGER_ADDITION | et_INTEGER_MULTIPLICATION | et_SEARCH;

            nullSpecOpt.Rule = NULL | NOT + NULL | Empty;

            var t_INT = ToTerm("INT");
            var t_CHAR = ToTerm("CHAR");
            var t_VARCHAR = ToTerm("VARCHAR");
            var t_NCHAR = ToTerm("NCHAR");
            var t_NVARCHAR = ToTerm("NVARCHAR");
            var t_TEXT = ToTerm("TEXT");
            var t_BINARY = ToTerm("BINARY");
            var t_VARBINARY = ToTerm("VARBINARY");
            var t_UNIQUEIDENTIFIER = ToTerm("UNIQUEIDENTIFIER");
            var t_DATETIME = ToTerm("DATETIME");
            var t_FLOAT = ToTerm("FLOAT");

            typeName.Rule = t_INT | t_CHAR | t_VARCHAR | t_NCHAR | t_NVARCHAR | t_TEXT | t_BINARY | t_VARBINARY |
                            t_UNIQUEIDENTIFIER | t_DATETIME | t_FLOAT;
            typeParamsOpt.Rule = "(" + number + ")" | "(" + number + comma + number + ")" | Empty;
            autoDefaultOpt.Rule = IDENTITY | DEFAULT + term | Empty;
            //constraintDef.Rule = CONSTRAINT + Id + constraintTypeOpt;
            //constraintListOpt.Rule = MakeStarRule(constraintListOpt, constraintDef);
            //constraintTypeOpt.Rule = PRIMARY + KEY + idlistPar | UNIQUE + idlistPar | NOT + NULL + idlistPar
            //                       | "Foreign" + KEY + idlistPar + "References" + Id + idlistPar;
            idlistPar.Rule = "(" + idlist + ")";
            idlist.Rule = MakePlusRule(idlist, comma, Id);

            //Create Index
            //createIndexStmt.Rule = CREATE + uniqueOpt + INDEX + Id + ON + Id + orderList + withClauseOpt;
            //uniqueOpt.Rule = Empty | UNIQUE;
            //orderList.Rule = MakePlusRule(orderList, comma, orderMember);
            //orderMember.Rule = Id + orderDirOpt;
            //orderDirOpt.Rule = Empty | "ASC" | "DESC";
            //withClauseOpt.Rule = Empty | WITH + PRIMARY | WITH + "Disallow" + NULL | WITH + "Ignore" + NULL;

            //Alter 
            alterStmt.Rule = ALTER + TABLE + Id + alterCmd;
            alterCmd.Rule = ALTER + COLUMN + fieldDef;
            //alterCmd.Rule = ADD + COLUMN + fieldDefList + constraintListOpt
            //              | ADD + constraintDef
            //              | DROP + COLUMN + Id
            //              | DROP + CONSTRAINT + Id;

            //Drop stmts
            //dropTableStmt.Rule = DROP + TABLE + Id;
            //dropIndexStmt.Rule = DROP + INDEX + Id + ON + Id;

            //Use stmt
            useStmt.Rule = USE + Id;

            //Insert stmt
            insertStmt.Rule = INSERT + intoOpt + Id + idlistPar + VALUES + insertDataList;
            insertData.Rule = "(" + exprList + ")"; //selectStmt | VALUES +
            intoOpt.Rule = Empty | INTO; //Into is optional in MSSQL
            insertDataList.Rule = MakePlusRule(insertDataList, comma, insertData); // new

            //Update stmt
            updateStmt.Rule = UPDATE + Id + SET + assignList + whereClauseOpt;
            assignList.Rule = MakePlusRule(assignList, comma, assignment);
            assignment.Rule = Id + "=" + expression;

            //Delete stmt
            deleteStmt.Rule = DELETE + FROM + Id + whereClauseOpt;

            //Select stmt
            selectStmt.Rule = SELECT + selRestrOpt + selList + fromClauseOpt + whereClauseOpt;
            // + intoClauseOpt + groupClauseOpt + havingClauseOpt + orderClauseOpt
            selRestrOpt.Rule = Empty | TOP + number | TOP + "(" + number + ")";
            //selRestrOpt.Rule = Empty | "ALL" | "DISTINCT";
            selList.Rule = columnItemList | "*";
            columnItemList.Rule = MakePlusRule(columnItemList, comma, columnItem);
            columnItem.Rule = columnSource + aliasOpt;
            aliasOpt.Rule = Empty | asOpt + Id;
            asOpt.Rule = Empty | AS;
            columnSource.Rule = expression;  // new 
                                             //| Id | aggregate ;
                                             //aggregate.Rule = aggregateName + "(" + aggregateArg + ")";
                                             //aggregateArg.Rule = expression | "*";
                                             //aggregateName.Rule = COUNT | "Avg" | "Min" | "Max" | "StDev" | "StDevP" | "Sum" | "Var" | "VarP";
                                             //intoClauseOpt.Rule = Empty | INTO + Id;
            fromClauseOpt.Rule = Empty | FROM + idlist; //+ joinChainOpt;
                                                        //joinChainOpt.Rule = Empty | joinKindOpt + JOIN + idlist + ON + Id + "=" + Id;
                                                        //joinKindOpt.Rule = Empty | "INNER" | "LEFT" | "RIGHT";
            whereClauseOpt.Rule = Empty | "WHERE" + expression;
            //groupClauseOpt.Rule = Empty | "GROUP" + BY + idlist;
            //havingClauseOpt.Rule = Empty | "HAVING" + expression;
            //orderClauseOpt.Rule = Empty | "ORDER" + BY + orderList;

            //Expression
            exprList.Rule = MakePlusRule(exprList, comma, expression);
            expression.Rule = term | unExpr | binExpr;// | betweenExpr; //-- BETWEEN doesn't work - yet; brings a few parsing conflicts 
            term.Rule = Id | string_literal | number | tuple | funCall; //| parSelectStmt;// | inStmt;
            tuple.Rule = "(" + exprList + ")";
            //parSelectStmt.Rule = "(" + selectStmt + ")";
            unExpr.Rule = unOp + term;
            unOp.Rule = NOT | "+" | "-" | "~";
            binExpr.Rule = expression + binOp + expression | "(" + expression + binOp + expression + ")";
            binOp.Rule = ToTerm("+") | "-" | "*" | "/" | "%" //arithmetic
                       | "&" | "|" | "^"                     //bit
                       | "=" | ">" | "<" | ">=" | "<=" | "<>" | "!=" | "!<" | "!>"
                       | "AND" | "OR" | "LIKE" | NOT + "LIKE" | "IN" | NOT + "IN";
            betweenExpr.Rule = expression + notOpt + "BETWEEN" + expression + "AND" + expression;
            notOpt.Rule = Empty | NOT;
            //funCall covers some psedo-operators and special forms like ANY(...), SOME(...), ALL(...), EXISTS(...), IN(...)
            //funCall.Rule = Id + "()";
            //funCall.Rule = Id + "(" + Empty | exprList + ")";
            funCall.Rule = Id + "(" + funArgs + ")";
            funArgs.Rule = Empty | exprList;
            //funArgs.Rule = selectStmt | exprList;
            //inStmt.Rule = expression + "IN" + "(" + exprList + ")";

            //Operators
            RegisterOperators(10, "*", "/", "%");
            RegisterOperators(9, "+", "-");
            RegisterOperators(8, "=", ">", "<", ">=", "<=", "<>", "!=", "!<", "!>", "LIKE", "IN");
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
            base.MarkTransient(stmt, term, asOpt, aliasOpt, stmtLine, expression, unOp, tuple);
            binOp.SetFlag(TermFlags.InheritPrecedence);

        }//constructor
    }
}
