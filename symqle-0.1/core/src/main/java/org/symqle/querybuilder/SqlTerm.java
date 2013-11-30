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

import org.symqle.common.Sql;
import org.symqle.common.SqlParameters;

/**
 * Enumerates terminal symbols of SQL grammar.
 *
 * @author Alexander Izyurov
 */
public enum SqlTerm {

    /**************************
     * Special characters
     */

    /**
     * &lt;not equals operator&gt; ::= &lt;&gt;
     */
      NE("<>"),
    /**
     * &lt;greater than or equals operator&gt; ::= &gt;
     */
       GE(">="),
    /**
     * &lt;less than or equals operator&gt; ::= &lt;
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
     * &lt;double quote&gt; ::= "
     */
       DOUBLE_QUOTE("\""),
    /**
     * &lt;percent&gt; ::= %
     */
       PERCENT("%"),
    /**
     * &lt;ampersand&gt; ::= &
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
    //                <reserved word>
    //          | <non-reserved word>

    //     <non-reserved word> ::=
    /**
     * <non-reserved word>
     */
    ADA,
    /**
     * <non-reserved word>
     */
    C,
    /**
     * <non-reserved word>
     */
    CATALOG_NAME ,
    /**
     * <non-reserved word>
     */
    CHARACTER_SET_CATALOG ,
    /**
     * <non-reserved word>
     */
    CHARACTER_SET_NAME,
    /**
     * <non-reserved word>
     */
    CHARACTER_SET_SCHEMA,
    /**
     * <non-reserved word>
     */
    CLASS_ORIGIN,
    /**
     * <non-reserved word>
     */
    COBOL,
    /**
     * <non-reserved word>
     */
    COLLATION_CATALOG,
    /**
     * <non-reserved word>
     */
    COLLATION_NAME,
    /**
     * <non-reserved word>
     */
    COLLATION_SCHEMA,
    /**
     * <non-reserved word>
     */
    COLUMN_NAME,
    /**
     * <non-reserved word>
     */
    COMMAND_FUNCTION,
    /**
     * <non-reserved word>
     */
    COMMITTED,
    /**
     * <non-reserved word>
     */
    CONDITION_NUMBER,
    /**
     * <non-reserved word>
     */
    CONNECTION_NAME,
    /**
     * <non-reserved word>
     */
    CONSTRAINT_CATALOG,
    /**
     * <non-reserved word>
     */
    CONSTRAINT_NAME,
    /**
     * <non-reserved word>
     */
    CONSTRAINT_SCHEMA,
    /**
     * <non-reserved word>
     */
    CURSOR_NAME,
    /**
     * <non-reserved word>
     */
    DATA,
    /**
     * <non-reserved word>
     */
    DATETIME_INTERVAL_CODE,
    /**
     * <non-reserved word>
     */
    DATETIME_INTERVAL_PRECISION,
    /**
     * <non-reserved word>
     */
    DYNAMIC_FUNCTION,
    /**
     * <non-reserved word>
     */
    FORTRAN,
    /**
     * <non-reserved word>
     */
    LENGTH,
    /**
     * <non-reserved word>
     */
    MESSAGE_LENGTH,
    /**
     * <non-reserved word>
     */
    MESSAGE_OCTET_LENGTH,
    /**
     * <non-reserved word>
     */
    MESSAGE_TEXT,
    /**
     * <non-reserved word>
     */
    MORE,
    /**
     * <non-reserved word>
     */
    MUMPS,
    /**
     * <non-reserved word>
     */

    /**
     * <reserved word>
     */
    ABSOLUTE,
    /**
     * <reserved word>
     */
    ACTION,
    /**
     * <reserved word>
     */
    ADD,
    /**
     * <reserved word>
     */
    /**
     * <reserved word>
     */
    ALL,
    /**
     * <reserved word>
     */
    ALLOCATE,
    /**
     * <reserved word>
     */
    ALTER,
    /**
     * <reserved word>
     */
    AND,
    /**
     * <reserved word>
     */
    ANY,
    /**
     * <reserved word>
     */
    ARE,
    /**
     * <reserved word>
     */
    AS,
    /**
     * <reserved word>
     */
    ASC,
    /**
     * <reserved word>
     */
    ASSERTION,
    /**
     * <reserved word>
     */
    AT,
    /**
     * <reserved word>
     */
    AUTHORIZATION,
    /**
     * <reserved word>
     */
    AVG,
    /**
     * <reserved word>
     */
    BEGIN,
    /**
     * <reserved word>
     */
    BETWEEN,
    /**
     * <reserved word>
     */
    BIT,
    /**
     * <reserved word>
     */
    BIT_LENGTH,
    /**
     * <reserved word>
     */
    BOTH,
    /**
     * <reserved word>
     */
    BY,
    /**
     * <reserved word>
     */
    CASCADE,
    /**
     * <reserved word>
     */
    CASCADED,
    /**
     * <reserved word>
     */
    CASE,
    /**
     * <reserved word>
     */
    CAST,
    /**
     * <reserved word>
     */
    CATALOG,
    /**
     * <reserved word>
     */
    CHAR,
    /**
     * <reserved word>
     */
    CHARACTER,
    /**
     * <reserved word>
     */
    CHAR_LENGTH,
    /**
     * <reserved word>
     */
    CHARACTER_LENGTH,
    /**
     * <reserved word>
     */
    CHECK,
    /**
     * <reserved word>
     */
    CLOSE,
    /**
     * <reserved word>
     */
    COALESCE,
    /**
     * <reserved word>
     */
    COLLATE,
    /**
     * <reserved word>
     */
    COLLATION,
    /**
     * <reserved word>
     */
    COLUMN,
    /**
     * <reserved word>
     */
    COMMIT,
    /**
     * <reserved word>
     */
    CONNECT,
    /**
     * <reserved word>
     */
    CONNECTION,
    /**
     * <reserved word>
     */
    CONSTRAINT,
    /**
     * <reserved word>
     */
    CONSTRAINTS,
    /**
     * <reserved word>
     */
    CONTINUE,
    /**
     * <reserved word>
     */
    CONVERT,
    /**
     * <reserved word>
     */
    CORRESPONDING,
    /**
     * <reserved word>
     */
    COUNT,
    /**
     * <reserved word>
     */
    CREATE,
    /**
     * <reserved word>
     */
    CROSS,
    /**
     * <reserved word>
     */
    CURRENT,
    /**
     * <reserved word>
     */
    CURRENT_DATE,
    /**
     * <reserved word>
     */
    CURRENT_TIME,
    /**
     * <reserved word>
     */
    CURRENT_TIMESTAMP,
    /**
     * <reserved word>
     */
    CURRENT_USER,
    /**
     * <reserved word>
     */
    CURSOR,
    /**
     * <reserved word>
     */
    DATE,
    /**
     * <reserved word>
     */
    DAY,
    /**
     * <reserved word>
     */
    DEALLOCATE,
    /**
     * <reserved word>
     */
    DEC,
    /**
     * <reserved word>
     */
    DECIMAL,
    /**
     * <reserved word>
     */
    DECLARE,
    /**
     * <reserved word>
     */
    DEFAULT,
    /**
     * <reserved word>
     */
    DEFERRABLE,
    /**
     * <reserved word>
     */
    DEFERRED,
    /**
     * <reserved word>
     */
    DELETE,
    /**
     * <reserved word>
     */
    DESC,
    /**
     * <reserved word>
     */
    DESCRIBE,
    /**
     * <reserved word>
     */
    DESCRIPTOR,
    /**
     * <reserved word>
     */
    DIAGNOSTICS,
    /**
     * <reserved word>
     */
    DISCONNECT,
    /**
     * <reserved word>
     */
    DISTINCT,
    /**
     * <reserved word>
     */
    DOMAIN,
    /**
     * <reserved word>
     */
    DOUBLE,
    /**
     * <reserved word>
     */
    DROP,
    /**
     * <reserved word>
     */
    ELSE,
    /**
     * <reserved word>
     */
    END,
    /**
     * <reserved word>
     */
    END_EXEC("END-EXEC"),
    /**
     * <reserved word>
     */
    ESCAPE,
    /**
     * <reserved word>
     */
    EXCEPT,
    /**
     * <reserved word>
     */
    EXCEPTION,
    /**
     * <reserved word>
     */
    EXEC,
    /**
     * <reserved word>
     */
    EXECUTE,
    /**
     * <reserved word>
     */
    EXISTS,
    /**
     * <reserved word>
     */
    EXTERNAL,
    /**
     * <reserved word>
     */
    /**
     * <reserved word>
     */
    EXTRACT,
    /**
     * <reserved word>
     */
    FALSE,
    /**
     * <reserved word>
     */
    FETCH,
    /**
     * <reserved word>
     */
    FIRST,
    /**
     * <reserved word>
     */
    FLOAT,
    /**
     * <reserved word>
     */
    FOR,
    /**
     * <reserved word>
     */
    FOREIGN,
    /**
     * <reserved word>
     */
    FOUND,
    /**
     * <reserved word>
     */
    FROM,
    /**
     * <reserved word>
     */
    FULL,
    /**
     * <reserved word>
     */
    GET,
    /**
     * <reserved word>
     */
    GLOBAL,
    /**
     * <reserved word>
     */
    GO,
    /**
     * <reserved word>
     */
    GOTO,
    /**
     * <reserved word>
     */
    GRANT,
    /**
     * <reserved word>
     */
    GROUP,
    /**
     * <reserved word>
     */
    HAVING,
    /**
     * <reserved word>
     */
    HOUR,
    /**
     * <reserved word>
     */
    IDENTITY,
    /**
     * <reserved word>
     */
    IMMEDIATE,
    /**
     * <reserved word>
     */
    IN,
    /**
     * <reserved word>
     */
    INDICATOR,
    /**
     * <reserved word>
     */
    INITIALLY,
    /**
     * <reserved word>
     */
    INNER,
    /**
     * <reserved word>
     */
    INPUT,
    /**
     * <reserved word>
     */
    INSENSITIVE,
    /**
     * <reserved word>
     */
    INSERT,
    /**
     * <reserved word>
     */
    INT,
    /**
     * <reserved word>
     */
    INTEGER,
    /**
     * <reserved word>
     */
    INTERSECT,
    /**
     * <reserved word>
     */
    INTERVAL,
    /**
     * <reserved word>
     */
    INTO,
    /**
     * <reserved word>
     */
    IS,
    /**
     * <reserved word>
     */
    ISOLATION,
    /**
     * <reserved word>
     */
    JOIN,
    /**
     * <reserved word>
     */
    KEY,
    /**
     * <reserved word>
     */
    LANGUAGE,
    /**
     * <reserved word>
     */
    LAST,
    /**
     * <reserved word>
     */
    LEADING,
    /**
     * <reserved word>
     */
    LEFT,
    /**
     * <reserved word>
     */
    LEVEL,
    /**
     * <reserved word>
     */
    LIKE,
    /**
     * <reserved word>
     */
    LOCAL,
    /**
     * <reserved word>
     */
    LOWER,
    /**
     * <reserved word>
     */
    MATCH,
    /**
     * <reserved word>
     */
    MAX,
    /**
     * <reserved word>
     */
    MIN,
    /**
     * <reserved word>
     */
    MINUTE,
    /**
     * <reserved word>
     */
    MODULE,
    /**
     * <reserved word>
     */
    MONTH,
    /**
     * <reserved word>
     */
    NAMES,
    /**
     * <reserved word>
     */
    NATIONAL,
    /**
     * <reserved word>
     */
    NATURAL,
    /**
     * <reserved word>
     */
    NCHAR,
    /**
     * <reserved word>
     */
    NEXT,
    /**
     * <reserved word>
     */
    NO,
    /**
     * <reserved word>
     */
    NOT,
    /**
     * <reserved word>
     */
    NULL,
    /**
     * <reserved word>
     */
    NULLS,
    /**
     * <reserved word>
     */
    NULLIF,
    /**
     * <reserved word>
     */
    NUMERIC,
    /**
     * <reserved word>
     */
    OCTET_LENGTH,
    /**
     * <reserved word>
     */
    OF,
    /**
     * <reserved word>
     */
    ON,
    /**
     * <reserved word>
     */
    ONLY,
    /**
     * <reserved word>
     */
    OPEN,
    /**
     * <reserved word>
     */
    OPTION,
    /**
     * <reserved word>
     */
    OR,
    /**
     * <reserved word>
     */
    ORDER,
    /**
     * <reserved word>
     */
    OUTER,
    /**
     * <reserved word>
     */
    OUTPUT,
    /**
     * <reserved word>
     */
    OVERLAPS,
    /**
     * <reserved word>
     */
    PAD ,
    /**
     * <reserved word>
     */
    PARTIAL,
    /**
     * <reserved word>
     */
    POSITION,
    /**
     * <reserved word>
     */
    PRECISION,
    /**
     * <reserved word>
     */
    PREPARE,
    /**
     * <reserved word>
     */
    PRESERVE,
    /**
     * <reserved word>
     */
    PRIMARY,
    /**
     * <reserved word>
     */
    PRIOR,
    /**
     * <reserved word>
     */
    PRIVILEGES,
    /**
     * <reserved word>
     */
    PROCEDURE,
    /**
     * <reserved word>
     */
    PUBLIC,
    /**
     * <reserved word>
     */
    READ,
    /**
     * <reserved word>
     */
    REAL,
    /**
     * <reserved word>
     */
    REFERENCES,
    /**
     * <reserved word>
     */
    RELATIVE,
    /**
     * <reserved word>
     */
    RESTRICT,
    /**
     * <reserved word>
     */
    REVOKE,
    /**
     * <reserved word>
     */
    RIGHT,
    /**
     * <reserved word>
     */
    ROLLBACK,
    /**
     * <reserved word>
     */
    ROWS,
    /**
     * <reserved word>
     */
    SCHEMA,
    /**
     * <reserved word>
     */
    SCROLL,
    /**
     * <reserved word>
     */
    SECOND,
    /**
     * <reserved word>
     */
    SECTION,
    /**
     * <reserved word>
     */
    SELECT,
    /**
     * <reserved word>
     */
    SESSION,
    /**
     * <reserved word>
     */
    SESSION_USER,
    /**
     * <reserved word>
     */
    SET,
    /**
     * <reserved word>
     */
    SIZE,
    /**
     * <reserved word>
     */
    SMALLINT,
    /**
     * <reserved word>
     */
    SOME,
    /**
     * <reserved word>
     */
    SPACE,
    /**
     * <reserved word>
     */
    SQL,
    /**
     * <reserved word>
     */
    SQLCODE,
    /**
     * <reserved word>
     */
    SQLERROR,
    /**
     * <reserved word>
     */
    SQLSTATE,
    /**
     * <reserved word>
     */
    SUBSTRING,
    /**
     * <reserved word>
     */
    SUM,
    /**
     * <reserved word>
     */
    SYSTEM_USER,
    /**
     * <reserved word>
     */
    TABLE,
    /**
     * <reserved word>
     */
    TEMPORARY,
    /**
     * <reserved word>
     */
    THEN,
    /**
     * <reserved word>
     */
    TIME,
    /**
     * <reserved word>
     */
    TIMESTAMP,
    /**
     * <reserved word>
     */
    TIMEZONE_HOUR,
    /**
     * <reserved word>
     */
    TIMEZONE_MINUTE,
    /**
     * <reserved word>
     */
    TO,
    /**
     * <reserved word>
     */
    TRAILING,
    /**
     * <reserved word>
     */
    TRANSACTION,
    /**
     * <reserved word>
     */
    TRANSLATE,
    /**
     * <reserved word>
     */
    TRANSLATION,
    /**
     * <reserved word>
     */
    TRIM,
    /**
     * <reserved word>
     */
    TRUE,
    /**
     * <reserved word>
     */
    UNION,
    /**
     * <reserved word>
     */
    UNIQUE,
    /**
     * <reserved word>
     */
    UNKNOWN,
    /**
     * <reserved word>
     */
    UPDATE,
    /**
     * <reserved word>
     */
    UPPER,
    /**
     * <reserved word>
     */
    USAGE,
    /**
     * <reserved word>
     */
    USER,
    /**
     * <reserved word>
     */
    USING,
    /**
     * <reserved word>
     */
    VALUE,
    /**
     * <reserved word>
     */
    VALUES,
    /**
     * <reserved word>
     */
    VARCHAR,
    /**
     * <reserved word>
     */
    VARYING,
    /**
     * <reserved word>
     */
    VIEW,
    /**
     * <reserved word>
     */
    WHEN,
    /**
     * <reserved word>
     */
    WHENEVER,
    /**
     * <reserved word>
     */
    WHERE,
    /**
     * <reserved word>
     */
    WITH,
    /**
     * <reserved word>
     */
    WORK,
    /**
     * <reserved word>
     */
    WRITE,
    /**
     * <reserved word>
     */
    YEAR,
    /**
     * <reserved word>
     */
    ZONE,

    // additional list reserved for future extensions
    /**
     * <reserved word>
     */
    AFTER,
    /**
     * <reserved word>
     */
    ALIAS,
    /**
     * <reserved word>
     */
    ASYNC,
    /**
     * <reserved word>
     */
    BEFORE,
    /**
     * <reserved word>
     */
    BOOLEAN,
    /**
     * <reserved word>
     */
    BREADTH,
    /**
     * <reserved word>
     */
    COMPLETION,
    /**
     * <reserved word>
     */
    CALL,
    /**
     * <reserved word>
     */
    CYCLE,
    /**
     * <reserved word>
     */
    DEPTH,
    /**
     * <reserved word>
     */
    DICTIONARY,
    /**
     * <reserved word>
     */
    EACH,
    /**
     * <reserved word>
     */
    ELSEIF,
    /**
     * <reserved word>
     */
    EQUALS,
    /**
     * <reserved word>
     */
    GENERAL,
    /**
     * <reserved word>
     */
    IF,
    /**
     * <reserved word>
     */
    IGNORE,
    /**
     * <reserved word>
     */
    LEAVE,
    /**
     * <reserved word>
     */
    LESS,
    /**
     * <reserved word>
     */
    LIMIT,
    /**
     * <reserved word>
     */
    LOOP,
    /**
     * <reserved word>
     */
    MODIFY,
    /**
     * <reserved word>
     */
    NEW,
    /**
     * <reserved word>
     */
    NONE,
    /**
     * <reserved word>
     */
    OBJECT,
    /**
     * <reserved word>
     */
    OFF,
    /**
     * <reserved word>
     */
    OFFSET,
    /**
     * <reserved word>
     */
    OID,
    /**
     * <reserved word>
     */
    OLD,
    /**
     * <reserved word>
     */
    OPERATION,
    /**
     * <reserved word>
     */
    OPERATORS,
    /**
     * <reserved word>
     */
    OTHERS,
    /**
     * <reserved word>
     */
    PARAMETERS,
    /**
     * <reserved word>
     */
    PENDANT,
    /**
     * <reserved word>
     */
    PREORDER,
    /**
     * <reserved word>
     */
    PRIVATE,
    /**
     * <reserved word>
     */
    PROTECTED,
    /**
     * <reserved word>
     */
    RECURSIVE,
    /**
     * <reserved word>
     */
    REF,
    /**
     * <reserved word>
     */
    REFERENCING,
    /**
     * <reserved word>
     */
    REPLACE,
    /**
     * <reserved word>
     */
    RESIGNAL,
    /**
     * <reserved word>
     */
    RETURN,
    /**
     * <reserved word>
     */
    RETURNS,
    /**
     * <reserved word>
     */
    ROLE,
    /**
     * <reserved word>
     */
    ROUTINE,
    /**
     * <reserved word>
     */
    ROW,
    /**
     * <reserved word>
     */
    SAVEPOINT,
    /**
     * <reserved word>
     */
    SEARCH,
    /**
     * <reserved word>
     */
    SENSITIVE,
    /**
     * <reserved word>
     */
    SEQUENCE,
    /**
     * <reserved word>
     */
    SIGNAL,
    /**
     * <reserved word>
     */
    SIMILAR,
    /**
     * <reserved word>
     */
    SQLEXCEPTION,
    /**
     * <reserved word>
     */
    SQLWARNING,
    /**
     * <reserved word>
     */
    STRUCTURE,
    /**
     * <reserved word>
     */
    TEST,
    /**
     * <reserved word>
     */
    THERE,
    /**
     * <reserved word>
     */
    TRIGGER,
    /**
     * <reserved word>
     */
    TYPE,
    /**
     * <reserved word>
     */
    UNDER,
    /**
     * <reserved word>
     */
    VARIABLE,
    /**
     * <reserved word>
     */
    VIRTUAL,
    /**
     * <reserved word>
     */
    VISIBLE,
    /**
     * <reserved word>
     */
    WAIT,
    /**
     * <reserved word>
     */
    WHILE,
    /**
     * <reserved word>
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

    public Sql toSql() {
        return new CustomSql(s);
    }

}
