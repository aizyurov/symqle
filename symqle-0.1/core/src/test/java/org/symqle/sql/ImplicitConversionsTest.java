package org.symqle.sql;

import junit.framework.TestCase;
import org.symqle.common.Mappers;

/**
 * @author lvovich
 */
public class ImplicitConversionsTest extends TestCase {

    public void testQueryExpression() throws Exception {
        final SelectSublist<Long> selectSublist = Symqle.z$SelectSublist$from$ValueExpression(person.id);
        assertEquals(Mappers.LONG, selectSublist.getMapper());
        final QueryBaseScalar<Long> queryBaseScalar = Symqle.z$QueryBaseScalar$from$SelectSublist(selectSublist);
        assertEquals(Mappers.LONG, queryBaseScalar.getMapper());
        final QuerySpecification<Long> querySpecification = Symqle.z$QuerySpecification$from$QueryBaseScalar(queryBaseScalar);
        assertEquals(Mappers.LONG, querySpecification.getMapper());
        final QueryPrimary<Long> queryPrimary = Symqle.z$QueryPrimary$from$QuerySpecification(querySpecification);
        assertEquals(Mappers.LONG, queryPrimary.getMapper());
        final QueryTerm<Long> queryTerm = Symqle.z$QueryTerm$from$QueryPrimary(queryPrimary);
        assertEquals(Mappers.LONG, queryTerm.getMapper());
        final QueryExpressionScalar<Long> queryExpressionScalar = Symqle.z$QueryExpressionScalar$from$QueryTerm(queryTerm);
        assertEquals(Mappers.LONG, queryExpressionScalar.getMapper());
        final ScalarSubquery<Long> scalarSubquery = Symqle.z$ScalarSubquery$from$QueryExpressionScalar(queryExpressionScalar);
        assertEquals(Mappers.LONG, scalarSubquery.getMapper());
        final QueryPrimary<Long> queryPrimary1 = Symqle.z$QueryPrimary$from$QueryExpressionScalar(queryExpressionScalar);
        assertEquals(Mappers.LONG, queryPrimary1.getMapper());
    }

    public void testNumericExpression() throws Exception {
        final ValueExpressionPrimary<Long> valueExpressionPrimary = Symqle.z$ValueExpressionPrimary$from$ColumnReference(person.id);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getMapper());
        final Factor<Long> factor = Symqle.z$Factor$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.LONG, factor.getMapper());
        final Term<Long> term = Symqle.z$Term$from$Factor(factor);
        assertEquals(Mappers.LONG, term.getMapper());
        final NumericExpression<Long> numericExpression = Symqle.z$NumericExpression$from$Term(term);
        assertEquals(Mappers.LONG, numericExpression.getMapper());
        final ValueExpression<Long> valueExpression = Symqle.z$ValueExpression$from$NumericExpression(numericExpression);
        assertEquals(Mappers.LONG, valueExpression.getMapper());
        final Predicand<Long> predicand = Symqle.z$Predicand$from$NumericExpression(numericExpression);
        assertEquals(Mappers.LONG, predicand.getMapper());
        final Predicand<Long> predicand1 = Symqle.z$Predicand$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.LONG, predicand1.getMapper());
        final ValueExpressionPrimary<Long> valueExpressionPrimary1 = Symqle.z$ValueExpressionPrimary$from$ValueExpression(valueExpression);
        assertEquals(Mappers.LONG, valueExpressionPrimary1.getMapper());
        final ValueExpression<Long> valueExpression1 = Symqle.z$ValueExpression$from$ValueExpressionPrimary(valueExpressionPrimary1);
        assertEquals(Mappers.LONG, valueExpression1.getMapper());

    }

    public void testStringExpression() throws Exception {
        final ValueExpressionPrimary<String> valueExpressionPrimary = Symqle.z$ValueExpressionPrimary$from$ColumnReference(person.name);
        assertEquals(Mappers.STRING, valueExpressionPrimary.getMapper());
        final CharacterFactor<String> characterFactor = Symqle.z$CharacterFactor$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.STRING, characterFactor.getMapper());
        final StringExpression<String> stringExpression = Symqle.z$StringExpression$from$CharacterFactor(characterFactor);
        assertEquals(Mappers.STRING, stringExpression.getMapper());
        final ValueExpression<String> valueExpression = Symqle.z$ValueExpression$from$StringExpression(stringExpression);
        assertEquals(Mappers.STRING, valueExpression.getMapper());
        final Predicand<String> predicand = Symqle.z$Predicand$from$StringExpression(stringExpression);
        assertEquals(Mappers.STRING, predicand.getMapper());
    }

    public void testValueExpressionPrimary() throws Exception {
        DynamicParameter<Long> parameter = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpressionPrimary<Long> valueExpressionPrimary = Symqle.z$ValueExpressionPrimary$from$DynamicParameterSpecification(parameter);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getMapper());
        final AbstractRoutineInvocation<Long> abs = SqlFunction.create("abs", Mappers.LONG).apply(person.id);
        final ValueExpressionPrimary<Long> valueExpressionPrimary1 = Symqle.z$ValueExpressionPrimary$from$RoutineInvocation(abs);
        assertEquals(Mappers.LONG, valueExpressionPrimary1.getMapper());
        final AbstractCastSpecification<Long> cast = person.id.cast("NUMBER");
        final ValueExpressionPrimary<Long> valueExpressionPrimary2 = Symqle.z$ValueExpressionPrimary$from$CastSpecification(cast);
        assertEquals(Mappers.LONG, valueExpressionPrimary2.getMapper());
    }

    public void testCaseExpression() throws Exception {
        final AbstractSearchedWhenClause<Long> whenClause = person.name.isNotNull().then(person.id);
        final SearchedWhenClauseBaseList<Long> baseList = Symqle.z$SearchedWhenClauseBaseList$from$SearchedWhenClause(whenClause);
        assertEquals(Mappers.LONG, baseList.getMapper());
        final SearchedWhenClauseList<Long> list = Symqle.z$SearchedWhenClauseList$from$SearchedWhenClauseBaseList(baseList);
        assertEquals(Mappers.LONG, list.getMapper());
        final CaseExpression<Long> caseExpression = Symqle.z$CaseExpression$from$SearchedWhenClauseList(list);
        assertEquals(Mappers.LONG, caseExpression.getMapper());
        final ValueExpressionPrimary<Long> valueExpressionPrimary = Symqle.z$ValueExpressionPrimary$from$CaseExpression(caseExpression);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getMapper());
        final ElseClause<Long> elseClause = Symqle.z$ElseClause$from$ValueExpression(person.id);
        assertEquals(Mappers.LONG, elseClause.getMapper());
    }

    public void testQueryBaseScalar() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final AggregateSelectSublist<Integer> aggregateSelectSublist = Symqle.z$AggregateSelectSublist$from$AggregateFunction(count);
        assertEquals(Mappers.INTEGER, count.getMapper());
        assertEquals(Mappers.INTEGER, aggregateSelectSublist.getMapper());
    }

    public void testQueryPrimary() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final AggregateSelectSublist<Integer> selectSublist = Symqle.z$AggregateSelectSublist$from$AggregateFunction(count);
        final QueryPrimary<Integer> queryPrimary = Symqle.z$QueryPrimary$from$AggregateSelectSublist(selectSublist);
        assertEquals(Mappers.INTEGER, selectSublist.getMapper());
        assertEquals(Mappers.INTEGER, queryPrimary.getMapper());
    }

    public void testAlternateWayToOrderBy() {
        // there are 2 ways from simple scalars to QueryExpression:
        // SelectSublist->QuerySpecification->QueryExpression
        // SelectSublist->SelectList->QueryExpression
        // Symqle chooses the first one; the second one is unreachable via
        // public API.
        // We ensure here that the second way is equivalent to the first

        {
            final Column<Long> column
                    = person.id;
            final String sql1 = column.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(column), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final DynamicParameter<Long> parameter
                    = DynamicParameter.create(Mappers.LONG, 1L);
            final String sql1 = parameter.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(parameter), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractCastSpecification<Long> cast = person.id.cast("DECIMAL(6)");
            final String sql1 = cast.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(cast), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractCharacterFactor<String> characterFactor = person.name.collate("latin1_general_ci");
            final String sql1 = characterFactor.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(characterFactor), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractNumericExpression<Number> numericExpression = person.id.add(2);
            final String sql1 = numericExpression.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(numericExpression), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractTerm<Number> term = person.id.mult(2);
            final String sql1 = term.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(term), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);

        }
        {
            final AbstractFactor<Long> factor = person.id.opposite();
            final String sql1 = factor.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(factor), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);

        }
        {
            final AbstractRoutineInvocation<Long> routineInvocation = SqlFunction.create("abs", Mappers.LONG).apply(person.id);
            final String sql1 = routineInvocation.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(routineInvocation), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractSearchedWhenClause<String> whenClause = person.id.gt(20L).then(person.name);
            final String sql1 = whenClause.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(whenClause), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList = person.id.gt(20L).then(person.name).orWhen(person.id.gt(1L).then(DynamicParameter.create(Mappers.STRING, "noname")));
            final String sql1 = whenClauseBaseList.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(whenClauseBaseList), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractSearchedWhenClauseList<String> whenClauseList = person.id.gt(20L).then(person.name).orWhen(person.id.gt(1L).then(DynamicParameter.create(Mappers.STRING, "noname"))).orElse(person.name);
            final String sql1 = whenClauseList.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(whenClauseList), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractStringExpression<String> stringExpression = person.name.concat("_abc");
            final String sql1 = stringExpression.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(stringExpression), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final AbstractValueExpression<Boolean> valueExpression = person.id.eq(1L).asValue();
            final String sql1 = valueExpression.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(valueExpression), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
        {
            final Person sample = new Person();
            final AbstractValueExpressionPrimary<Long> valueExpressionPrimary = sample.id.queryValue();
            final String sql1 = valueExpressionPrimary.orderBy(person.id).show(new GenericDialect());
            final String sql2 = Symqle.orderBy(Symqle.z$QueryExpression$from$QueryBase(valueExpressionPrimary), person.id).show(new GenericDialect());
            assertEquals(sql1, sql2);
        }
    }
//
//    public void testAggregateQuerySpecification() throws Exception {
//        final Symqle symqle = Symqle.get();
//        final AbstractAggregateFunction<Integer> count = person.id.count();
//        final AggregateQuerySpecification<Integer> querySpecification = Symqle.z$AggregateQuerySpecification$from$AggregateQueryBase(count);
//        assertEquals(Mappers.INTEGER,  querySpecification.getMapper());
//    }
//
//    public void testAggregateQueryBase() throws Exception {
//        final Symqle symqle = Symqle.get();
//        final AbstractAggregateFunction<Integer> count = person.id.count();
//        final AggregateQueryBase<Integer> aggregateQueryBase = Symqle.z$AggregateQueryBase$from$AggregateSelectSublist(count);
//        assertEquals(Mappers.INTEGER, aggregateQueryBase.getMapper());
//    }
//
    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();

}
