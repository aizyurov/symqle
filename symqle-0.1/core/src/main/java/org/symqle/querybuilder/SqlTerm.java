/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.querybuilder;

import org.symqle.common.SqlBuilder;
import org.symqle.common.SqlParameters;

import java.sql.SQLException;

/**
 * Enumerates terminal symbols of SQL grammar.
 *
 * @author Alexander Izyurov
 */
public enum SqlTerm implements SqlBuilder {

    /**************************
     * Special characters
     */

    /**
     * &lt;not equals operator&gt; ::= &lt;&gt;
     */
      NE("<>"),
    /**
     * &lt;greater than or equals operator&gt; ::= &gt;=
     */
       GE(">="),
    /**
     * &lt;less than or equals operator&gt; ::= &lt;=
     */
       LE("<="),
    /**
     * &lt;concatenation operator&gt ::= ||
     */
       CONCAT("||"),
    /**
     * &lt;double period&gt; ::= ..
     */
       DOUBLE_PERIOD(".."),
    /**
     * &lt;double quote&gt; ::= &quot;
     */
       DOUBLE_QUOTE("\""),
    /**
     * &lt;percent&gt; ::= %
     */
       PERCENT("%"),
    /**
     * &lt;ampersand&gt; ::= &amp;
     */
       AMPERSAND("&"),
    /**
     * &lt;quote&gt; ::= '
     */
       QUOTE("'"),
    /**
     * &lt;left paren&gt; ::= (
     */
       LEFT_PAREN("("),
    /**
     * &lt;right paren&gt; ::= )
     */
       RIGHT_PAREN(")"),
    /**
     * &lt;asterisk&gt; ::= *
     */
       ASTERISK("*"),
    /**
     * &lt;plus sign&gt; ::= +
     */
       PLUS("+"),
    /**
     * &lt;comma&gt; ::= ,
     */
       COMMA(","),
    /**
     * &lt;minus sign&gt; ::= -
     */
       MINUS("-"),
    /**
     * &lt;period&gt; ::= .
     */
       PERIOD("."),
    /**
     * &lt;solidus&gt; ::= /
     */
       SOLIDUS("/"),
    /**
     * &lt;colon&gt; ::= :
     */
       COLON(":"),
    /**
     * &lt;semicolon&gt; ::= ;
     */
       SEMICOLON(";"),
    /**
     * &lt;less than operator&gt; ::= &lt;
     */
       LT("<"),
    /**
     * &lt;equals operator&gt; ::= =
     */
       EQ("="),
    /**
     * &lt;greater than operator&gt; ::= &gt;
     */
       GT(">"),
    /**
     * &lt;question mark&gt; ::= ?
     */
       QUESTION("?"),
    /**
     * &lt;left bracket&gt; ::= [
     */
       LEFT_BRACKET("["),
    /**
     * &lt;right bracket&gt; ::= ]
     */
       RIGHT_BRACKET("]"),
    /**
     * &lt;underscore&gt; ::= _
     */
       UNDERSCORE("_"),
    /**
     * &lt;vertical bar&gt; ::= |
     */
     VERTICAL_BAR("|"),

    /******************************
     * keywords
     */

    //          <key word> ::=
    //                &lt;reserved word&gt;
    //          | &lt;non-reserved word&gt;

    //     &lt;non-reserved word&gt; ::=
    /**
     * &lt;non-reserved word&gt;
     */
    ADA,
    /**
     * &lt;non-reserved word&gt;
     */
    C,
    /**
     * &lt;non-reserved word&gt;
     */
    CATALOG_NAME ,
    /**
     * &lt;non-reserved word&gt;
     */
    CHARACTER_SET_CATALOG ,
    /**
     * &lt;non-reserved word&gt;
     */
    CHARACTER_SET_NAME,
    /**
     * &lt;non-reserved word&gt;
     */
    CHARACTER_SET_SCHEMA,
    /**
     * &lt;non-reserved word&gt;
     */
    CLASS_ORIGIN,
    /**
     * &lt;non-reserved word&gt;
     */
    COBOL,
    /**
     * &lt;non-reserved word&gt;
     */
    COLLATION_CATALOG,
    /**
     * &lt;non-reserved word&gt;
     */
    COLLATION_NAME,
    /**
     * &lt;non-reserved word&gt;
     */
    COLLATION_SCHEMA,
    /**
     * &lt;non-reserved word&gt;
     */
    COLUMN_NAME,
    /**
     * &lt;non-reserved word&gt;
     */
    COMMAND_FUNCTION,
    /**
     * &lt;non-reserved word&gt;
     */
    COMMITTED,
    /**
     * &lt;non-reserved word&gt;
     */
    CONDITION_NUMBER,
    /**
     * &lt;non-reserved word&gt;
     */
    CONNECTION_NAME,
    /**
     * &lt;non-reserved word&gt;
     */
    CONSTRAINT_CATALOG,
    /**
     * &lt;non-reserved word&gt;
     */
    CONSTRAINT_NAME,
    /**
     * &lt;non-reserved word&gt;
     */
    CONSTRAINT_SCHEMA,
    /**
     * &lt;non-reserved word&gt;
     */
    CURSOR_NAME,
    /**
     * &lt;non-reserved word&gt;
     */
    DATA,
    /**
     * &lt;non-reserved word&gt;
     */
    DATETIME_INTERVAL_CODE,
    /**
     * &lt;non-reserved word&gt;
     */
    DATETIME_INTERVAL_PRECISION,
    /**
     * &lt;non-reserved word&gt;
     */
    DYNAMIC_FUNCTION,
    /**
     * &lt;non-reserved word&gt;
     */
    FORTRAN,
    /**
     * &lt;non-reserved word&gt;
     */
    LENGTH,
    /**
     * &lt;non-reserved word&gt;
     */
    MESSAGE_LENGTH,
    /**
     * &lt;non-reserved word&gt;
     */
    MESSAGE_OCTET_LENGTH,
    /**
     * &lt;non-reserved word&gt;
     */
    MESSAGE_TEXT,
    /**
     * &lt;non-reserved word&gt;
     */
    MORE,
    /**
     * &lt;non-reserved word&gt;
     */
    MUMPS,
    /**
     * &lt;non-reserved word&gt;
     */

    /**
     * &lt;reserved word&gt;
     */
    ABSOLUTE,
    /**
     * &lt;reserved word&gt;
     */
    ACTION,
    /**
     * &lt;reserved word&gt;
     */
    ADD,
    /**
     * &lt;reserved word&gt;
     */
    /**
     * &lt;reserved word&gt;
     */
    ALL,
    /**
     * &lt;reserved word&gt;
     */
    ALLOCATE,
    /**
     * &lt;reserved word&gt;
     */
    ALTER,
    /**
     * &lt;reserved word&gt;
     */
    AND,
    /**
     * &lt;reserved word&gt;
     */
    ANY,
    /**
     * &lt;reserved word&gt;
     */
    ARE,
    /**
     * &lt;reserved word&gt;
     */
    AS,
    /**
     * &lt;reserved word&gt;
     */
    ASC,
    /**
     * &lt;reserved word&gt;
     */
    ASSERTION,
    /**
     * &lt;reserved word&gt;
     */
    AT,
    /**
     * &lt;reserved word&gt;
     */
    AUTHORIZATION,
    /**
     * &lt;reserved word&gt;
     */
    AVG,
    /**
     * &lt;reserved word&gt;
     */
    BEGIN,
    /**
     * &lt;reserved word&gt;
     */
    BETWEEN,
    /**
     * &lt;reserved word&gt;
     */
    BIT,
    /**
     * &lt;reserved word&gt;
     */
    BIT_LENGTH,
    /**
     * &lt;reserved word&gt;
     */
    BOTH,
    /**
     * &lt;reserved word&gt;
     */
    BY,
    /**
     * &lt;reserved word&gt;
     */
    CASCADE,
    /**
     * &lt;reserved word&gt;
     */
    CASCADED,
    /**
     * &lt;reserved word&gt;
     */
    CASE,
    /**
     * &lt;reserved word&gt;
     */
    CAST,
    /**
     * &lt;reserved word&gt;
     */
    CATALOG,
    /**
     * &lt;reserved word&gt;
     */
    CHAR,
    /**
     * &lt;reserved word&gt;
     */
    CHARACTER,
    /**
     * &lt;reserved word&gt;
     */
    CHAR_LENGTH,
    /**
     * &lt;reserved word&gt;
     */
    CHARACTER_LENGTH,
    /**
     * &lt;reserved word&gt;
     */
    CHECK,
    /**
     * &lt;reserved word&gt;
     */
    CLOSE,
    /**
     * &lt;reserved word&gt;
     */
    COALESCE,
    /**
     * &lt;reserved word&gt;
     */
    COLLATE,
    /**
     * &lt;reserved word&gt;
     */
    COLLATION,
    /**
     * &lt;reserved word&gt;
     */
    COLUMN,
    /**
     * &lt;reserved word&gt;
     */
    COMMIT,
    /**
     * &lt;reserved word&gt;
     */
    CONNECT,
    /**
     * &lt;reserved word&gt;
     */
    CONNECTION,
    /**
     * &lt;reserved word&gt;
     */
    CONSTRAINT,
    /**
     * &lt;reserved word&gt;
     */
    CONSTRAINTS,
    /**
     * &lt;reserved word&gt;
     */
    CONTINUE,
    /**
     * &lt;reserved word&gt;
     */
    CONVERT,
    /**
     * &lt;reserved word&gt;
     */
    CORRESPONDING,
    /**
     * &lt;reserved word&gt;
     */
    COUNT,
    /**
     * &lt;reserved word&gt;
     */
    CREATE,
    /**
     * &lt;reserved word&gt;
     */
    CROSS,
    /**
     * &lt;reserved word&gt;
     */
    CURRENT,
    /**
     * &lt;reserved word&gt;
     */
    CURRENT_DATE,
    /**
     * &lt;reserved word&gt;
     */
    CURRENT_TIME,
    /**
     * &lt;reserved word&gt;
     */
    CURRENT_TIMESTAMP,
    /**
     * &lt;reserved word&gt;
     */
    CURRENT_USER,
    /**
     * &lt;reserved word&gt;
     */
    CURSOR,
    /**
     * &lt;reserved word&gt;
     */
    DATE,
    /**
     * &lt;reserved word&gt;
     */
    DAY,
    /**
     * &lt;reserved word&gt;
     */
    DEALLOCATE,
    /**
     * &lt;reserved word&gt;
     */
    DEC,
    /**
     * &lt;reserved word&gt;
     */
    DECIMAL,
    /**
     * &lt;reserved word&gt;
     */
    DECLARE,
    /**
     * &lt;reserved word&gt;
     */
    DEFAULT,
    /**
     * &lt;reserved word&gt;
     */
    DEFERRABLE,
    /**
     * &lt;reserved word&gt;
     */
    DEFERRED,
    /**
     * &lt;reserved word&gt;
     */
    DELETE,
    /**
     * &lt;reserved word&gt;
     */
    DESC,
    /**
     * &lt;reserved word&gt;
     */
    DESCRIBE,
    /**
     * &lt;reserved word&gt;
     */
    DESCRIPTOR,
    /**
     * &lt;reserved word&gt;
     */
    DIAGNOSTICS,
    /**
     * &lt;reserved word&gt;
     */
    DISCONNECT,
    /**
     * &lt;reserved word&gt;
     */
    DISTINCT,
    /**
     * &lt;reserved word&gt;
     */
    DOMAIN,
    /**
     * &lt;reserved word&gt;
     */
    DOUBLE,
    /**
     * &lt;reserved word&gt;
     */
    DROP,
    /**
     * &lt;reserved word&gt;
     */
    ELSE,
    /**
     * &lt;reserved word&gt;
     */
    END,
    /**
     * &lt;reserved word&gt;
     */
    END_EXEC("END-EXEC"),
    /**
     * &lt;reserved word&gt;
     */
    ESCAPE,
    /**
     * &lt;reserved word&gt;
     */
    EXCEPT,
    /**
     * &lt;reserved word&gt;
     */
    EXCEPTION,
    /**
     * &lt;reserved word&gt;
     */
    EXEC,
    /**
     * &lt;reserved word&gt;
     */
    EXECUTE,
    /**
     * &lt;reserved word&gt;
     */
    EXISTS,
    /**
     * &lt;reserved word&gt;
     */
    EXTERNAL,
    /**
     * &lt;reserved word&gt;
     */
    /**
     * &lt;reserved word&gt;
     */
    EXTRACT,
    /**
     * &lt;reserved word&gt;
     */
    FALSE,
    /**
     * &lt;reserved word&gt;
     */
    FETCH,
    /**
     * &lt;reserved word&gt;
     */
    FIRST,
    /**
     * &lt;reserved word&gt;
     */
    FLOAT,
    /**
     * &lt;reserved word&gt;
     */
    FOR,
    /**
     * &lt;reserved word&gt;
     */
    FOREIGN,
    /**
     * &lt;reserved word&gt;
     */
    FOUND,
    /**
     * &lt;reserved word&gt;
     */
    FROM,
    /**
     * &lt;reserved word&gt;
     */
    FULL,
    /**
     * &lt;reserved word&gt;
     */
    GET,
    /**
     * &lt;reserved word&gt;
     */
    GLOBAL,
    /**
     * &lt;reserved word&gt;
     */
    GO,
    /**
     * &lt;reserved word&gt;
     */
    GOTO,
    /**
     * &lt;reserved word&gt;
     */
    GRANT,
    /**
     * &lt;reserved word&gt;
     */
    GROUP,
    /**
     * &lt;reserved word&gt;
     */
    HAVING,
    /**
     * &lt;reserved word&gt;
     */
    HOUR,
    /**
     * &lt;reserved word&gt;
     */
    IDENTITY,
    /**
     * &lt;reserved word&gt;
     */
    IMMEDIATE,
    /**
     * &lt;reserved word&gt;
     */
    IN,
    /**
     * &lt;reserved word&gt;
     */
    INDICATOR,
    /**
     * &lt;reserved word&gt;
     */
    INITIALLY,
    /**
     * &lt;reserved word&gt;
     */
    INNER,
    /**
     * &lt;reserved word&gt;
     */
    INPUT,
    /**
     * &lt;reserved word&gt;
     */
    INSENSITIVE,
    /**
     * &lt;reserved word&gt;
     */
    INSERT,
    /**
     * &lt;reserved word&gt;
     */
    INT,
    /**
     * &lt;reserved word&gt;
     */
    INTEGER,
    /**
     * &lt;reserved word&gt;
     */
    INTERSECT,
    /**
     * &lt;reserved word&gt;
     */
    INTERVAL,
    /**
     * &lt;reserved word&gt;
     */
    INTO,
    /**
     * &lt;reserved word&gt;
     */
    IS,
    /**
     * &lt;reserved word&gt;
     */
    ISOLATION,
    /**
     * &lt;reserved word&gt;
     */
    JOIN,
    /**
     * &lt;reserved word&gt;
     */
    KEY,
    /**
     * &lt;reserved word&gt;
     */
    LANGUAGE,
    /**
     * &lt;reserved word&gt;
     */
    LAST,
    /**
     * &lt;reserved word&gt;
     */
    LEADING,
    /**
     * &lt;reserved word&gt;
     */
    LEFT,
    /**
     * &lt;reserved word&gt;
     */
    LEVEL,
    /**
     * &lt;reserved word&gt;
     */
    LIKE,
    /**
     * &lt;reserved word&gt;
     */
    LOCAL,
    /**
     * &lt;reserved word&gt;
     */
    LOWER,
    /**
     * &lt;reserved word&gt;
     */
    MATCH,
    /**
     * &lt;reserved word&gt;
     */
    MAX,
    /**
     * &lt;reserved word&gt;
     */
    MIN,
    /**
     * &lt;reserved word&gt;
     */
    MINUTE,
    /**
     * &lt;reserved word&gt;
     */
    MODULE,
    /**
     * &lt;reserved word&gt;
     */
    MONTH,
    /**
     * &lt;reserved word&gt;
     */
    NAMES,
    /**
     * &lt;reserved word&gt;
     */
    NATIONAL,
    /**
     * &lt;reserved word&gt;
     */
    NATURAL,
    /**
     * &lt;reserved word&gt;
     */
    NCHAR,
    /**
     * &lt;reserved word&gt;
     */
    NEXT,
    /**
     * &lt;reserved word&gt;
     */
    NO,
    /**
     * &lt;reserved word&gt;
     */
    NOT,
    /**
     * &lt;reserved word&gt;
     */
    NULL,
    /**
     * &lt;reserved word&gt;
     */
    NULLS,
    /**
     * &lt;reserved word&gt;
     */
    NULLIF,
    /**
     * &lt;reserved word&gt;
     */
    NUMERIC,
    /**
     * &lt;reserved word&gt;
     */
    OCTET_LENGTH,
    /**
     * &lt;reserved word&gt;
     */
    OF,
    /**
     * &lt;reserved word&gt;
     */
    ON,
    /**
     * &lt;reserved word&gt;
     */
    ONLY,
    /**
     * &lt;reserved word&gt;
     */
    OPEN,
    /**
     * &lt;reserved word&gt;
     */
    OPTION,
    /**
     * &lt;reserved word&gt;
     */
    OR,
    /**
     * &lt;reserved word&gt;
     */
    ORDER,
    /**
     * &lt;reserved word&gt;
     */
    OUTER,
    /**
     * &lt;reserved word&gt;
     */
    OUTPUT,
    /**
     * &lt;reserved word&gt;
     */
    OVERLAPS,
    /**
     * &lt;reserved word&gt;
     */
    PAD ,
    /**
     * &lt;reserved word&gt;
     */
    PARTIAL,
    /**
     * &lt;reserved word&gt;
     */
    POSITION,
    /**
     * &lt;reserved word&gt;
     */
    PRECISION,
    /**
     * &lt;reserved word&gt;
     */
    PREPARE,
    /**
     * &lt;reserved word&gt;
     */
    PRESERVE,
    /**
     * &lt;reserved word&gt;
     */
    PRIMARY,
    /**
     * &lt;reserved word&gt;
     */
    PRIOR,
    /**
     * &lt;reserved word&gt;
     */
    PRIVILEGES,
    /**
     * &lt;reserved word&gt;
     */
    PROCEDURE,
    /**
     * &lt;reserved word&gt;
     */
    PUBLIC,
    /**
     * &lt;reserved word&gt;
     */
    READ,
    /**
     * &lt;reserved word&gt;
     */
    REAL,
    /**
     * &lt;reserved word&gt;
     */
    REFERENCES,
    /**
     * &lt;reserved word&gt;
     */
    RELATIVE,
    /**
     * &lt;reserved word&gt;
     */
    RESTRICT,
    /**
     * &lt;reserved word&gt;
     */
    REVOKE,
    /**
     * &lt;reserved word&gt;
     */
    RIGHT,
    /**
     * &lt;reserved word&gt;
     */
    ROLLBACK,
    /**
     * &lt;reserved word&gt;
     */
    ROWS,
    /**
     * &lt;reserved word&gt;
     */
    SCHEMA,
    /**
     * &lt;reserved word&gt;
     */
    SCROLL,
    /**
     * &lt;reserved word&gt;
     */
    SECOND,
    /**
     * &lt;reserved word&gt;
     */
    SECTION,
    /**
     * &lt;reserved word&gt;
     */
    SELECT,
    /**
     * &lt;reserved word&gt;
     */
    SESSION,
    /**
     * &lt;reserved word&gt;
     */
    SESSION_USER,
    /**
     * &lt;reserved word&gt;
     */
    SET,
    /**
     * &lt;reserved word&gt;
     */
    SIZE,
    /**
     * &lt;reserved word&gt;
     */
    SMALLINT,
    /**
     * &lt;reserved word&gt;
     */
    SOME,
    /**
     * &lt;reserved word&gt;
     */
    SPACE,
    /**
     * &lt;reserved word&gt;
     */
    SQL,
    /**
     * &lt;reserved word&gt;
     */
    SQLCODE,
    /**
     * &lt;reserved word&gt;
     */
    SQLERROR,
    /**
     * &lt;reserved word&gt;
     */
    SQLSTATE,
    /**
     * &lt;reserved word&gt;
     */
    SUBSTRING,
    /**
     * &lt;reserved word&gt;
     */
    SUM,
    /**
     * &lt;reserved word&gt;
     */
    SYSTEM_USER,
    /**
     * &lt;reserved word&gt;
     */
    TABLE,
    /**
     * &lt;reserved word&gt;
     */
    TEMPORARY,
    /**
     * &lt;reserved word&gt;
     */
    THEN,
    /**
     * &lt;reserved word&gt;
     */
    TIME,
    /**
     * &lt;reserved word&gt;
     */
    TIMESTAMP,
    /**
     * &lt;reserved word&gt;
     */
    TIMEZONE_HOUR,
    /**
     * &lt;reserved word&gt;
     */
    TIMEZONE_MINUTE,
    /**
     * &lt;reserved word&gt;
     */
    TO,
    /**
     * &lt;reserved word&gt;
     */
    TRAILING,
    /**
     * &lt;reserved word&gt;
     */
    TRANSACTION,
    /**
     * &lt;reserved word&gt;
     */
    TRANSLATE,
    /**
     * &lt;reserved word&gt;
     */
    TRANSLATION,
    /**
     * &lt;reserved word&gt;
     */
    TRIM,
    /**
     * &lt;reserved word&gt;
     */
    TRUE,
    /**
     * &lt;reserved word&gt;
     */
    UNION,
    /**
     * &lt;reserved word&gt;
     */
    UNIQUE,
    /**
     * &lt;reserved word&gt;
     */
    UNKNOWN,
    /**
     * &lt;reserved word&gt;
     */
    UPDATE,
    /**
     * &lt;reserved word&gt;
     */
    UPPER,
    /**
     * &lt;reserved word&gt;
     */
    USAGE,
    /**
     * &lt;reserved word&gt;
     */
    USER,
    /**
     * &lt;reserved word&gt;
     */
    USING,
    /**
     * &lt;reserved word&gt;
     */
    VALUE,
    /**
     * &lt;reserved word&gt;
     */
    VALUES,
    /**
     * &lt;reserved word&gt;
     */
    VARCHAR,
    /**
     * &lt;reserved word&gt;
     */
    VARYING,
    /**
     * &lt;reserved word&gt;
     */
    VIEW,
    /**
     * &lt;reserved word&gt;
     */
    WHEN,
    /**
     * &lt;reserved word&gt;
     */
    WHENEVER,
    /**
     * &lt;reserved word&gt;
     */
    WHERE,
    /**
     * &lt;reserved word&gt;
     */
    WITH,
    /**
     * &lt;reserved word&gt;
     */
    WORK,
    /**
     * &lt;reserved word&gt;
     */
    WRITE,
    /**
     * &lt;reserved word&gt;
     */
    YEAR,
    /**
     * &lt;reserved word&gt;
     */
    ZONE,

    // additional list reserved for future extensions
    /**
     * &lt;reserved word&gt;
     */
    AFTER,
    /**
     * &lt;reserved word&gt;
     */
    ALIAS,
    /**
     * &lt;reserved word&gt;
     */
    ASYNC,
    /**
     * &lt;reserved word&gt;
     */
    BEFORE,
    /**
     * &lt;reserved word&gt;
     */
    BOOLEAN,
    /**
     * &lt;reserved word&gt;
     */
    BREADTH,
    /**
     * &lt;reserved word&gt;
     */
    COMPLETION,
    /**
     * &lt;reserved word&gt;
     */
    CALL,
    /**
     * &lt;reserved word&gt;
     */
    CYCLE,
    /**
     * &lt;reserved word&gt;
     */
    DEPTH,
    /**
     * &lt;reserved word&gt;
     */
    DICTIONARY,
    /**
     * &lt;reserved word&gt;
     */
    EACH,
    /**
     * &lt;reserved word&gt;
     */
    ELSEIF,
    /**
     * &lt;reserved word&gt;
     */
    EQUALS,
    /**
     * &lt;reserved word&gt;
     */
    GENERAL,
    /**
     * &lt;reserved word&gt;
     */
    IF,
    /**
     * &lt;reserved word&gt;
     */
    IGNORE,
    /**
     * &lt;reserved word&gt;
     */
    LEAVE,
    /**
     * &lt;reserved word&gt;
     */
    LESS,
    /**
     * &lt;reserved word&gt;
     */
    LIMIT,
    /**
     * &lt;reserved word&gt;
     */
    LOOP,
    /**
     * &lt;reserved word&gt;
     */
    MODIFY,
    /**
     * &lt;reserved word&gt;
     */
    NEW,
    /**
     * &lt;reserved word&gt;
     */
    NONE,
    /**
     * &lt;reserved word&gt;
     */
    OBJECT,
    /**
     * &lt;reserved word&gt;
     */
    OFF,
    /**
     * &lt;reserved word&gt;
     */
    OFFSET,
    /**
     * &lt;reserved word&gt;
     */
    OID,
    /**
     * &lt;reserved word&gt;
     */
    OLD,
    /**
     * &lt;reserved word&gt;
     */
    OPERATION,
    /**
     * &lt;reserved word&gt;
     */
    OPERATORS,
    /**
     * &lt;reserved word&gt;
     */
    OTHERS,
    /**
     * &lt;reserved word&gt;
     */
    PARAMETERS,
    /**
     * &lt;reserved word&gt;
     */
    PENDANT,
    /**
     * &lt;reserved word&gt;
     */
    PREORDER,
    /**
     * &lt;reserved word&gt;
     */
    PRIVATE,
    /**
     * &lt;reserved word&gt;
     */
    PROTECTED,
    /**
     * &lt;reserved word&gt;
     */
    RECURSIVE,
    /**
     * &lt;reserved word&gt;
     */
    REF,
    /**
     * &lt;reserved word&gt;
     */
    REFERENCING,
    /**
     * &lt;reserved word&gt;
     */
    REPLACE,
    /**
     * &lt;reserved word&gt;
     */
    RESIGNAL,
    /**
     * &lt;reserved word&gt;
     */
    RETURN,
    /**
     * &lt;reserved word&gt;
     */
    RETURNS,
    /**
     * &lt;reserved word&gt;
     */
    ROLE,
    /**
     * &lt;reserved word&gt;
     */
    ROUTINE,
    /**
     * &lt;reserved word&gt;
     */
    ROW,
    /**
     * &lt;reserved word&gt;
     */
    SAVEPOINT,
    /**
     * &lt;reserved word&gt;
     */
    SEARCH,
    /**
     * &lt;reserved word&gt;
     */
    SENSITIVE,
    /**
     * &lt;reserved word&gt;
     */
    SEQUENCE,
    /**
     * &lt;reserved word&gt;
     */
    SIGNAL,
    /**
     * &lt;reserved word&gt;
     */
    SIMILAR,
    /**
     * &lt;reserved word&gt;
     */
    SQLEXCEPTION,
    /**
     * &lt;reserved word&gt;
     */
    SQLWARNING,
    /**
     * &lt;reserved word&gt;
     */
    STRUCTURE,
    /**
     * &lt;reserved word&gt;
     */
    TEST,
    /**
     * &lt;reserved word&gt;
     */
    THERE,
    /**
     * &lt;reserved word&gt;
     */
    TRIGGER,
    /**
     * &lt;reserved word&gt;
     */
    TYPE,
    /**
     * &lt;reserved word&gt;
     */
    UNDER,
    /**
     * &lt;reserved word&gt;
     */
    VARIABLE,
    /**
     * &lt;reserved word&gt;
     */
    VIRTUAL,
    /**
     * &lt;reserved word&gt;
     */
    VISIBLE,
    /**
     * &lt;reserved word&gt;
     */
    WAIT,
    /**
     * &lt;reserved word&gt;
     */
    WHILE,
    /**
     * &lt;reserved word&gt;
     */
    WITHOUT
    ;

    /**
     * No-arg constructor
     */
    SqlTerm() {
        s = super.toString();
    }

    private final String s;

    /**
     * Force string to be used as sql text
     * @param s the text
     */
    SqlTerm(final String s) {
        this.s = s;
    }

    @Override
    public void appendTo(StringBuilder builder) {
        builder.append(s);
    }

    @Override
    public final void setParameters(SqlParameters p) throws SQLException {
    }

    @Override
    public char firstChar() {
        return s.charAt(0);
    }


}
