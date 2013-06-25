package org.simqle.sql;

import junit.framework.TestCase;
import org.simqle.Mappers;

/**
 * @author lvovich
 */
public class ImplicitConversionsTest extends TestCase {

    public void testQueryExpression() throws Exception {
        final Simqle simqle = Simqle.get();
        final SelectSublist<Long> selectSublist = simqle.z$SelectSublist$from$ValueExpression(person.id);
        assertEquals(Mappers.LONG, selectSublist.getMapper());
        final QueryBaseScalar<Long> queryBaseScalar = simqle.z$QueryBaseScalar$from$SelectSublist(selectSublist);
        assertEquals(Mappers.LONG, queryBaseScalar.getMapper());
        final QuerySpecification<Long> querySpecification = simqle.z$QuerySpecification$from$QueryBaseScalar(queryBaseScalar);
        assertEquals(Mappers.LONG, querySpecification.getMapper());
        final QueryPrimary<Long> queryPrimary = simqle.z$QueryPrimary$from$QuerySpecification(querySpecification);
        assertEquals(Mappers.LONG, queryPrimary.getMapper());
        final QueryTerm<Long> queryTerm = simqle.z$QueryTerm$from$QueryPrimary(queryPrimary);
        assertEquals(Mappers.LONG, queryTerm.getMapper());
        final QueryExpressionScalar<Long> queryExpressionScalar = simqle.z$QueryExpressionScalar$from$QueryTerm(queryTerm);
        assertEquals(Mappers.LONG, queryExpressionScalar.getMapper());
        final ScalarSubquery<Long> scalarSubquery = simqle.z$ScalarSubquery$from$QueryExpressionScalar(queryExpressionScalar);
        assertEquals(Mappers.LONG, scalarSubquery.getMapper());
        final QueryPrimary<Long> queryPrimary1 = simqle.z$QueryPrimary$from$QueryExpressionScalar(queryExpressionScalar);
        assertEquals(Mappers.LONG, queryPrimary1.getMapper());
    }

    public void testNumericExpression() throws Exception {
        final Simqle simqle = Simqle.get();
        final ValueExpressionPrimary<Long> valueExpressionPrimary = simqle.z$ValueExpressionPrimary$from$ColumnReference(person.id);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getMapper());
        final Factor<Long> factor = simqle.z$Factor$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.LONG, factor.getMapper());
        final Term<Long> term = simqle.z$Term$from$Factor(factor);
        assertEquals(Mappers.LONG, term.getMapper());
        final NumericExpression<Long> numericExpression = simqle.z$NumericExpression$from$Term(term);
        assertEquals(Mappers.LONG, numericExpression.getMapper());
        final ValueExpression<Long> valueExpression = simqle.z$ValueExpression$from$NumericExpression(numericExpression);
        assertEquals(Mappers.LONG, valueExpression.getMapper());
        final Predicand<Long> predicand = simqle.z$Predicand$from$NumericExpression(numericExpression);
        assertEquals(Mappers.LONG, predicand.getMapper());
        final Predicand<Long> predicand1 = simqle.z$Predicand$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.LONG, predicand1.getMapper());
        final ValueExpressionPrimary<Long> valueExpressionPrimary1 = simqle.z$ValueExpressionPrimary$from$ValueExpression(valueExpression);
        assertEquals(Mappers.LONG, valueExpressionPrimary1.getMapper());
        final ValueExpression<Long> valueExpression1 = simqle.z$ValueExpression$from$ValueExpressionPrimary(valueExpressionPrimary1);
        assertEquals(Mappers.LONG, valueExpression1.getMapper());

    }

    public void testStringExpression() throws Exception {
        final Simqle simqle = Simqle.get();
        final ValueExpressionPrimary<String> valueExpressionPrimary = simqle.z$ValueExpressionPrimary$from$ColumnReference(person.name);
        assertEquals(Mappers.STRING, valueExpressionPrimary.getMapper());
        final CharacterFactor<String> characterFactor = simqle.z$CharacterFactor$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.STRING, characterFactor.getMapper());
        final StringExpression<String> stringExpression = simqle.z$StringExpression$from$CharacterFactor(characterFactor);
        assertEquals(Mappers.STRING, stringExpression.getMapper());
        final ValueExpression<String> valueExpression = simqle.z$ValueExpression$from$StringExpression(stringExpression);
        assertEquals(Mappers.STRING, valueExpression.getMapper());
        final Predicand<String> predicand = simqle.z$Predicand$from$StringExpression(stringExpression);
        assertEquals(Mappers.STRING, predicand.getMapper());
    }

    public void testValueExpressionPrimary() throws Exception {
        final Simqle simqle = Simqle.get();
        DynamicParameter<Long> parameter = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpressionPrimary<Long> valueExpressionPrimary = simqle.z$ValueExpressionPrimary$from$DynamicParameterSpecification(parameter);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getMapper());
        final AbstractRoutineInvocation<Long> abs = SqlFunction.create("abs", Mappers.LONG).apply(person.id);
        final ValueExpressionPrimary<Long> valueExpressionPrimary1 = simqle.z$ValueExpressionPrimary$from$RoutineInvocation(abs);
        assertEquals(Mappers.LONG, valueExpressionPrimary1.getMapper());
        final AbstractCastSpecification<Long> cast = person.id.cast("NUMBER");
        final ValueExpressionPrimary<Long> valueExpressionPrimary2 = simqle.z$ValueExpressionPrimary$from$CastSpecification(cast);
        assertEquals(Mappers.LONG, valueExpressionPrimary2.getMapper());
    }

    public void testCaseExpression() throws Exception {
        final Simqle simqle = Simqle.get();
        final AbstractSearchedWhenClause<Long> whenClause = person.name.isNotNull().then(person.id);
        final SearchedWhenClauseBaseList<Long> baseList = simqle.z$SearchedWhenClauseBaseList$from$SearchedWhenClause(whenClause);
        assertEquals(Mappers.LONG, baseList.getMapper());
        final SearchedWhenClauseList<Long> list = simqle.z$SearchedWhenClauseList$from$SearchedWhenClauseBaseList(baseList);
        assertEquals(Mappers.LONG, list.getMapper());
        final CaseExpression<Long> caseExpression = simqle.z$CaseExpression$from$SearchedWhenClauseList(list);
        assertEquals(Mappers.LONG, caseExpression.getMapper());
        final ValueExpressionPrimary<Long> valueExpressionPrimary = simqle.z$ValueExpressionPrimary$from$CaseExpression(caseExpression);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getMapper());
        final ElseClause<Long> elseClause = simqle.z$ElseClause$from$ValueExpression(person.id);
        assertEquals(Mappers.LONG, elseClause.getMapper());
    }

    public void testQueryBaseScalar() throws Exception {
        final Simqle simqle = Simqle.get();
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final AggregateSelectSublist<Integer> aggregateSelectSublist = simqle.z$AggregateSelectSublist$from$AggregateFunction(count);
        assertEquals(Mappers.INTEGER, count.getMapper());
        assertEquals(Mappers.INTEGER, aggregateSelectSublist.getMapper());
    }
//
//    public void testAggregateQuerySpecification() throws Exception {
//        final Simqle simqle = Simqle.get();
//        final AbstractAggregateFunction<Integer> count = person.id.count();
//        final AggregateQuerySpecification<Integer> querySpecification = simqle.z$AggregateQuerySpecification$from$AggregateQueryBase(count);
//        assertEquals(Mappers.INTEGER,  querySpecification.getMapper());
//    }
//
//    public void testAggregateQueryBase() throws Exception {
//        final Simqle simqle = Simqle.get();
//        final AbstractAggregateFunction<Integer> count = person.id.count();
//        final AggregateQueryBase<Integer> aggregateQueryBase = simqle.z$AggregateQueryBase$from$AggregateSelectSublist(count);
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
