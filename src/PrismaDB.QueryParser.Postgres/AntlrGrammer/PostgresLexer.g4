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

lexer grammar PostgresLexer;

channels { ERRORCHANNEL }

// SKIP

SPACE:                               [ \t\r\n]+    -> channel(HIDDEN);
COMMENT_INPUT:                       '/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:                        (
                                       ('-- ' | '#') ~[\r\n]* ('\r'? '\n' | EOF) 
                                       | '--' ('\r'? '\n' | EOF) 
                                     ) -> channel(HIDDEN);


// Keywords
// Common Keywords

ADD:                                 'ADD';
ALTER:                               'ALTER';
AND:                                 'AND';
AS:                                  'AS';
ASC:                                 'ASC';
AUTO_INCREMENT:                      'AUTO_INCREMENT';
BY:                                  'BY';
COLUMN:                              'COLUMN';
COLUMNS:                             'COLUMNS';
CREATE:                              'CREATE';
CROSS:                               'CROSS';
DEFAULT:                             'DEFAULT';
DELETE:                              'DELETE';
DESC:                                'DESC';
DROP:                                'DROP';
ESCAPE:                              'ESCAPE';
FOR:                                 'FOR';
FROM:                                'FROM';
FULL:                                'FULL';
GROUP:                               'GROUP';
IN:                                  'IN';
INDEX:                               'INDEX';
INNER:                               'INNER';
INSERT:                              'INSERT';
INTO:                                'INTO';
IS:                                  'IS';
JOIN:                                'JOIN';
KEY:                                 'KEY';
LEFT:                                'LEFT';
LIKE:                                'LIKE';
LIMIT:                               'LIMIT';
NOT:                                 'NOT';
NULL_LITERAL:                        'NULL';
ON:                                  'ON';
OR:                                  'OR';
ORDER:                               'ORDER';
OUTER:                               'OUTER';
PRIMARY:                             'PRIMARY';
RIGHT:                               'RIGHT';
SELECT:                              'SELECT';
SET:                                 'SET';
SHOW:                                'SHOW';
TABLE:                               'TABLE';
TABLES:                              'TABLES';
TO:                                  'TO';
TYPE:                                'TYPE';
UPDATE:                              'UPDATE';
USE:                                 'USE';
VALUES:                              'VALUES';
WHERE:                               'WHERE';
WITH:                                'WITH';


// DATA TYPE Keywords

INT2:                                'INT2';
SMALLINT:                            'SMALLINT';
INT4:                                'INT4';
INT:                                 'INT';
INTEGER:                             'INTEGER';
INT8:                                'INT8';
BIGINT:                              'BIGINT';
FLOAT4:                              'FLOAT4';
REAL:                                'REAL';
FLOAT8:                              'FLOAT8';
DOUBLE_PRECISION:                    'DOUBLE' SPACE 'PRECISION';
DECIMAL:                             'DECIMAL';
DATE:                                'DATE';
TIMESTAMP:                           'TIMESTAMP';
CHAR:                                'CHAR';
VARCHAR:                             'VARCHAR';
BYTEA:                               'BYTEA';
TEXT:                                'TEXT';
SERIAL:                              'SERIAL';


// Encryption keywords

ENCRYPTED:                           'ENCRYPTED';
ADDITION:                            'ADDITION';
SEARCH:                              'SEARCH';
STORE:                               'STORE';
MULTIPLICATION:                      'MULTIPLICATION';
RANGE:                               'RANGE';
WILDCARD:                            'WILDCARD';


// Command keywords

PRISMADB:                            'PRISMADB';
EXPORT:                              'EXPORT';
SETTINGS:                            'SETTINGS';
KEYS:                                'KEYS';
ENCRYPT:                             'ENCRYPT';
DECRYPT:                             'DECRYPT';
STATUS:                              'STATUS';
REBALANCE:                           'REBALANCE';
SAVE:                                'SAVE';
OPETREE:                             'OPETREE';
LOAD:                                'LOAD';
SCHEMA:                              'SCHEMA';
BYPASS:                              'BYPASS';
LICENSE:                             'LICENSE';
REFRESH:                             'REFRESH';


// Common function names

SUM:                                 'SUM';
AVG:                                 'AVG';
ABS:                                 'ABS';
COUNT:                               'COUNT';
MIN:                                 'MIN';
MAX:                                 'MAX';
NOW:                                 'NOW';
TIMEOFDAY:                           'TIMEOFDAY';
STDDEV_SAMP:                         'STDDEV_SAMP';
LINREG:                              'LINREG';


// Common function Keywords

CURRENT_TIMESTAMP:                   'CURRENT_TIMESTAMP';
LOCALTIMESTAMP:                      'LOCALTIMESTAMP';


// Operators
// Operators. Arithmetics

STAR:                                '*';
DIVIDE:                              '/';
PLUS:                                '+';
MINUSMINUS:                          '--';
MINUS:                               '-';


// Operators. Comparation

EQUAL_SYMBOL:                        '=';
GREATER_SYMBOL:                      '>';
LESS_SYMBOL:                         '<';
EXCLAMATION_SYMBOL:                  '!';


// Constructors symbols

DOT:                                 '.';
LR_BRACKET:                          '(';
RR_BRACKET:                          ')';
COMMA:                               ',';
SEMI:                                ';';
AT_SIGN:                             '@';
SINGLE_QUOTE_SYMB:                   '\'';
DOUBLE_QUOTE_SYMB:                   '"';
REVERSE_QUOTE_SYMB:                  '`';
COLON_SYMB:                          ':';



// Literal Primitives


STRING_LITERAL:                      SQUOTA_STRING;
INT_LITERAL:                         '-'? DEC_DIGIT+;
DECIMAL_LITERAL:                     '-'? (DEC_DIGIT+)? '.' DEC_DIGIT+;
HEXADECIMAL_LITERAL:                 'E' '\'' '\\' '\\' 'X' (HEX_DIGIT HEX_DIGIT)+ '\'';

PARAMETER:                           '$' DEC_DIGIT+;


// Hack for dotID
// Prevent recognize string:         .123somelatin AS ((.123), FLOAT_LITERAL), ((somelatin), ID)
//  it must recoginze:               .123somelatin AS ((.), DOT), (123somelatin, ID)

DOT_ID:                              '.' ID_LITERAL;



// Identifiers

ID:                                  ID_LITERAL;
DOUBLE_QUOTE_ID:                     '"' ~('"')+ '"';


// Fragments for Literal primitives

fragment ID_LITERAL:                 [A-Z_$0-9]*?[A-Z_$]+?[A-Z_$0-9]*;
fragment DQUOTA_STRING:              '"' ( '\\'. | '""' | ~('"'| '\\') )* '"';
fragment SQUOTA_STRING:              '\'' ( '\'\'' | ~'\'')* '\'';
fragment HEX_DIGIT:                  [0-9A-F];
fragment DEC_DIGIT:                  [0-9];



// Last tokens must generate Errors

ERROR_RECONGNIGION:                  .    -> channel(ERRORCHANNEL);