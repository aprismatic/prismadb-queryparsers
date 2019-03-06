/*
MySQL (Positive Technologies) grammar
The MIT License (MIT).
Copyright (c) 2015-2017, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
Copyright (c) 2017, Ivan Khudyashev (IHudyashov@ptsecurity.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

parser grammar MySqlParser;

options { tokenVocab=MySqlLexer; }


// Top Level Description

root
    : sqlStatements? MINUSMINUS? EOF
    ;

sqlStatements
    : (sqlStatement MINUSMINUS? SEMI | emptyStatement)*
    (sqlStatement (MINUSMINUS? SEMI)? | emptyStatement)
    ;

sqlStatement
    : ddlStatement | dmlStatement | dclStatement | utilityStatement
    ;

emptyStatement
    : SEMI
    ;

ddlStatement
    : createTable | alterTable | dropTable
    ;

dmlStatement
    : selectStatement | insertStatement | updateStatement
    | deleteStatement
    ;

dclStatement
	: exportSettingsCommand | updateKeysCommand | encyrptCommand
    | decryptCommand | registerUserCommand
	;

utilityStatement
	: useStatement | showTablesStatement | showColumnsStatement
	;


// Data Definition Language

//    Create statements

createTable
    : CREATE TABLE
      tableName
      createDefinitions
    ;

// details

createDefinitions
    : '(' createDefinition (',' createDefinition)* ')'
    ;

createDefinition
    : uid columnDefinition                                          #columnDeclaration
    ;

columnDefinition
    : dataType
	( ENCRYPTED encryptionOptions? )?
	nullNotnull?
	(
	  ( DEFAULT defaultValue )
      | AUTO_INCREMENT PRIMARY KEY
	)?
    ;


//    Alter statements

alterTable
    : ALTER TABLE
      tableName
      alterSpecification
    ;

// details

alterSpecification
    : MODIFY COLUMN?
      uid columnDefinition                                          #alterByModifyColumn
    ;


//    Drop statements

dropTable
    : DROP TABLE
      tables
    ;


// Data Manipulation Language

//    Primary DML Statements


deleteStatement
    : singleDeleteStatement
    ;

insertStatement
    : INSERT
      INTO? tableName
      (
        ('(' columns=uidList ')')? insertStatementValue
      )
    ;

selectStatement
    : SELECT selectElements
      fromClause? whereClause? groupByClause? orderByClause? limitClause?
	;

updateStatement
    : singleUpdateStatement
    ;

// details

insertStatementValue
    : insertFormat=VALUES
      '(' expressions ')'
        (',' '(' expressions ')')*
    ;

updatedElement
    : fullColumnName '=' expression
    ;

//    Detailed DML Statements

singleDeleteStatement
    : DELETE
    FROM tableName
      (WHERE expression)?
    ;

singleUpdateStatement
    : UPDATE tableName
      SET updatedElement (',' updatedElement)*
      (WHERE expression)?
    ;

// details

orderByClause
    : ORDER BY orderByExpression (',' orderByExpression)*
    ;

orderByExpression
    : expression order=(ASC | DESC)?
    ;

tableSources
    : tableSourceItem (',' tableSourceItem)*
    ;

tableSourceItem
    : tableName
      (AS? alias=uid)?
    ;

joinPart
    : (INNER | CROSS)? JOIN tableSourceItem
      (
        ON expression
      )?                                                            #innerJoin
    | (LEFT | RIGHT) OUTER? JOIN tableSourceItem
        (
          ON expression
        )                                                           #outerJoin
    ;

selectElements
    : (star='*' | selectElement ) (',' selectElement)*
    ;

selectElement
    : uid '.' '*'                                                   #selectStarElement
    | fullColumnName (AS? uid)?                                     #selectColumnElement
    | functionCall (AS? uid)?                                       #selectFunctionElement
    | expression (AS? uid)?                                         #selectExpressionElement
    ;

fromClause
    : FROM tableSources joinPart*
    ;

whereClause
	: WHERE whereExpr=expression
	;

groupByClause
	: GROUP BY groupByItem (',' groupByItem)*
	;

groupByItem
    : expression
    ;

limitClause
    : LIMIT
    (
      limit=intLiteral
    )
    ;


// Utility Statements


useStatement
    : USE databaseName
    ;

showTablesStatement
    : SHOW TABLES
    ;

showColumnsStatement
    : SHOW COLUMNS FROM tableName
    ;


// Prisma/DB Data Control Language

exportSettingsCommand
	: PRISMADB EXPORT SETTINGS TO stringLiteral
	;

updateKeysCommand
	: PRISMADB UPDATE KEYS STATUS?
	;

encyrptCommand
	: PRISMADB ENCRYPT fullColumnName encryptionOptions? STATUS?
	;

decryptCommand
	: PRISMADB DECRYPT fullColumnName STATUS?
	;

registerUserCommand
	: PRISMADB REGISTER USER
    user=stringLiteral PASSWORD password=stringLiteral
	;


// Common Clauses

//    DB Objects

databaseName
	: uid
	;

tableName
    : uid
    ;

fullColumnName
    : uid dottedId?
    ;

mysqlVariable
    : GLOBAL_ID
    ;

uid
    : simpleId
    | REVERSE_QUOTE_ID
    ;

simpleId
    : ID
    | keywordsCanBeId
    ;

dottedId
    : DOT_ID
    | '.' uid
    ;


//    Literals

intLiteral
    : INT_LITERAL
    ;

decimalLiteral
    : DECIMAL_LITERAL
    ;

stringLiteral
    : STRING_LITERAL
    ;

hexadecimalLiteral
    : HEXADECIMAL_LITERAL
    ;

nullNotnull
    : NOT? (NULL_LITERAL | NULL_SPEC_LITERAL)
    ;

constant
    : intLiteral | stringLiteral
    | decimalLiteral | hexadecimalLiteral
    | nullLiteral=(NULL_LITERAL | NULL_SPEC_LITERAL)
    ;


//    Data Types

dataType
    : typeName=(
      CHAR | VARCHAR | TEXT
      )
      lengthOneDimension?                                           #stringDataType
    | typeName=(
        TINYINT | SMALLINT | INT | BIGINT | DOUBLE |
        DATE | TIMESTAMP | DATETIME | BLOB
      )                                                             #simpleDataType
    | typeName=(
        BINARY | VARBINARY
      )
      lengthOneDimension?                                           #dimensionDataType
    |  typeName=ENUM
      '(' stringLiteral (',' stringLiteral)* ')'                    #collectionDataType
    ;

lengthOneDimension
    : '(' intLiteral ')'
    ;

lengthTwoDimension
    : '(' intLiteral ',' intLiteral ')'
    ;

lengthTwoOptionalDimension
    : '(' intLiteral (',' intLiteral)? ')'
    ;


//    Common Lists

uidList
    : uid (',' uid)*
    ;

tables
    : tableName (',' tableName)*
    ;

expressions
    : expression (',' expression)*
    ;

constants
    : constant (',' constant)*
    ;


//    Common Expressons

defaultValue
    : constant
    | currentTimestamp
    ;

currentTimestamp
    :
    (
      (CURRENT_TIMESTAMP)
    )
    ;


//    Functions

functionCall
    : specificFunction                                              #specificFunctionCall
    | scalarFunctionName '(' functionArgs? ')'                      #scalarFunctionCall
    | uid '(' functionArgs? ')'                                     #udfFunctionCall
    ;

specificFunction
    : (
      CURRENT_TIMESTAMP
      )                                                             #simpleFunctionCall
    ;

scalarFunctionName
    : SUM | AVG | ABS | COUNT | MIN | MAX
    | NOW | DATE | UTC_TIMESTAMP | TIMEDIFF
    ;

functionArgs
    : functionArg
    (
      ','
      functionArg
    )*
    ;

functionArg
    : constant | fullColumnName | functionCall | expression | star='*'
    ;


//    Expressions, predicates

// Simplified approach for expression
expression
    : notOperator=(NOT | '!') expression                            #notExpression
    | expression logicalOperator expression                         #logicalExpression
    | predicate                                                     #predicateExpression
	| '(' (expression) ')'                                          #nestedExpression
    ;

predicate
    : predicate NOT? IN '(' (expressions) ')'                       #inPredicate
    | predicate IS nullNotnull                                      #isNullPredicate
    | left=predicate comparisonOperator right=predicate             #binaryComparasionPredicate
    | predicate NOT? LIKE predicate (ESCAPE stringLiteral)?         #likePredicate
    | expressionAtom                                                #expressionAtomPredicate
	| addSubExpression                                              #mathExpressionPredicate
	| '(' (predicate) ')'                                           #nestedPredicate
    ;

addSubExpression
	: mulDivExpression                                              #nestedMulDivExpression
	| left=addSubExpression addSubOperator right=addSubExpression   #addSubExpressionAtom
	| '(' (addSubExpression) ')'                                    #nestedAddSubExpression
	;

mulDivExpression
	: expressionAtom                                                #simpleExpressionAtom
	| left=mulDivExpression mulDivOperator right=mulDivExpression   #mulDivExpressionAtom
	| '(' (addSubExpression) ')'                                    #nestedAddSubExpressionInMulDiv
	;

// Add in ASTVisitor nullNotnull in constant
expressionAtom
    : constant                                                      #constantExpressionAtom
    | fullColumnName                                                #fullColumnNameExpressionAtom
    | functionCall                                                  #functionCallExpressionAtom
    | mysqlVariable                                                 #mysqlVariableExpressionAtom
    | unaryOperator expressionAtom                                  #unaryExpressionAtom
	| '(' (expressionAtom) ')'                                      #nestedExpressionAtom
    ;

unaryOperator
    : '!' | NOT
    ;

comparisonOperator
    : '=' | '>' | '<' | '<' '=' | '>' '='
    | '<' '>' | '!' '='
    ;

logicalOperator
    : AND | OR
    ;

addSubOperator
    : '+' | '-'
    ;

mulDivOperator
    : '*' | '/'
    ;

keywordsCanBeId
    : AUTO_INCREMENT | ENCRYPTED | MODIFY
	| ADDITION | SEARCH | STORE | MULTIPLICATION | WILDCARD
    | PRISMADB | EXPORT | SETTINGS | ENCRYPT | DECRYPT
    | STATUS | REGISTER | USER | PASSWORD ;



//    Encryption

encryptionOptions
	: FOR '(' encryptionType (',' encryptionType)* ')'
	;

encryptionType
	: ADDITION | SEARCH | STORE | MULTIPLICATION
    | RANGE | WILDCARD
	;