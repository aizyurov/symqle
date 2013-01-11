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
        assertEquals(Mappers.LONG, selectSublist.getElementMapper());
        final QueryBaseScalar<Long> queryBaseScalar = simqle.z$QueryBaseScalar$from$SelectSublist(selectSublist);
        assertEquals(Mappers.LONG, queryBaseScalar.getElementMapper());
        final QuerySpecification<Long> querySpecification = simqle.z$QuerySpecification$from$QueryBaseScalar(queryBaseScalar);
        assertEquals(Mappers.LONG, querySpecification.getElementMapper());
        final QueryPrimary<Long> queryPrimary = simqle.z$QueryPrimary$from$QuerySpecification(querySpecification);
        assertEquals(Mappers.LONG, queryPrimary.getElementMapper());
        final QueryTerm<Long> queryTerm = simqle.z$QueryTerm$from$QueryPrimary(queryPrimary);
        assertEquals(Mappers.LONG, queryTerm.getElementMapper());
        final QueryExpressionScalar<Long> queryExpressionScalar = simqle.z$QueryExpressionScalar$from$QueryTerm(queryTerm);
        assertEquals(Mappers.LONG, queryExpressionScalar.getElementMapper());
        final ScalarSubquery<Long> scalarSubquery = simqle.z$ScalarSubquery$from$QueryExpressionScalar(queryExpressionScalar);
        assertEquals(Mappers.LONG, scalarSubquery.getElementMapper());
        final QueryPrimary<Long> queryPrimary1 = simqle.z$QueryPrimary$from$QueryExpressionScalar(queryExpressionScalar);
        assertEquals(Mappers.LONG, queryPrimary1.getElementMapper());
    }

    public void testNumericExpression() throws Exception {
        final Simqle simqle = Simqle.get();
        final ValueExpressionPrimary<Long> valueExpressionPrimary = simqle.z$ValueExpressionPrimary$from$ColumnReference(person.id);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getElementMapper());
        final Factor<Long> factor = simqle.z$Factor$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.LONG, factor.getElementMapper());
        final Term<Long> term = simqle.z$Term$from$Factor(factor);
        assertEquals(Mappers.LONG, term.getElementMapper());
        final NumericExpression<Long> numericExpression = simqle.z$NumericExpression$from$Term(term);
        assertEquals(Mappers.LONG, numericExpression.getElementMapper());
        final ValueExpression<Long> valueExpression = simqle.z$ValueExpression$from$NumericExpression(numericExpression);
        assertEquals(Mappers.LONG, valueExpression.getElementMapper());
        final Predicand<Long> predicand = simqle.z$Predicand$from$NumericExpression(numericExpression);
        assertEquals(Mappers.LONG, predicand.getElementMapper());
        final Predicand<Long> predicand1 = simqle.z$Predicand$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.LONG, predicand1.getElementMapper());
        final ValueExpressionPrimary<Long> valueExpressionPrimary1 = simqle.z$ValueExpressionPrimary$from$ValueExpression(valueExpression);
        assertEquals(Mappers.LONG, valueExpressionPrimary1.getElementMapper());
        final ValueExpression<Long> valueExpression1 = simqle.z$ValueExpression$from$ValueExpressionPrimary(valueExpressionPrimary1);
        assertEquals(Mappers.LONG, valueExpression1.getElementMapper());

    }

    public void testStringExpression() throws Exception {
        final Simqle simqle = Simqle.get();
        final ValueExpressionPrimary<String> valueExpressionPrimary = simqle.z$ValueExpressionPrimary$from$ColumnReference(person.name);
        assertEquals(Mappers.STRING, valueExpressionPrimary.getElementMapper());
        final StringExpression<String> stringExpression = simqle.z$StringExpression$from$ValueExpressionPrimary(valueExpressionPrimary);
        assertEquals(Mappers.STRING, stringExpression.getElementMapper());
        final ValueExpression<String> valueExpression = simqle.z$ValueExpression$from$StringExpression(stringExpression);
        assertEquals(Mappers.STRING, valueExpression.getElementMapper());
        final Predicand<String> predicand = simqle.z$Predicand$from$StringExpression(stringExpression);
        assertEquals(Mappers.STRING, predicand.getElementMapper());
    }

    public void testValueExpressionPrimary() throws Exception {
        final Simqle simqle = Simqle.get();
        DynamicParameter<Long> parameter = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpressionPrimary<Long> valueExpressionPrimary = simqle.z$ValueExpressionPrimary$from$DynamicParameterSpecification(parameter);
        assertEquals(Mappers.LONG, valueExpressionPrimary.getElementMapper());
        final AbstractRoutineInvocation<Long> abs = SqlFunction.create("abs", Mappers.LONG).apply(person.id);
        final ValueExpressionPrimary<Long> valueExpressionPrimary1 = simqle.z$ValueExpressionPrimary$from$RoutineInvocation(abs);
        assertEquals(Mappers.LONG, valueExpressionPrimary1.getElementMapper());
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();

}
