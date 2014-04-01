package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.*;
import org.symqle.testset.AbstractCastSpecificationTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class CastSpecificationTest extends AbstractIntegrationTestBase implements AbstractCastSpecificationTestSet {



    private AbstractCastSpecification<Double> createCast(final Employee employee) {
        return employee.salary.cast("DECIMAL(6,2)");
    }

    @Override
    public void test_adapt_CastSpecification() throws Exception {
        final Employee employee = new Employee();
        final AbstractCastSpecification<Double> cast = createCast(employee);
        final AbstractCastSpecification<Double> adaptor = AbstractCastSpecification.adapt(cast);
        final List<Double> list = adaptor.list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).add(100).map(CoreMappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(1600.0, 3100.0, 2100.0, 2100.0, 3100.0), list);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.add(createCast(employee)).map(CoreMappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(3000.0, 6000.0, 4000.0, 4000.0, 6000.0), list);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).add(employee.salary).map(CoreMappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(3000.0, 6000.0, 4000.0, 4000.0, 6000.0), list);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.in(employee.salary.asInValueList().append(createCast(employee))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.in(createCast(employee).asInValueList()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_asPredicate_() throws Exception {
        final Employee employee = new Employee();
        final String castTarget = getDatabaseName().equals("MySQL") ? "DECIMAL(1)" : "BOOLEAN";
        final List<String> list = employee.lastName
                .where(employee.retired.cast(castTarget).asPredicate())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(createCast(employee).asc(), employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createCast(employee).avg().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(2300.0, list.get(0).doubleValue());

    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).cast("DECIMAL(5,1)").list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }


    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.cast("CHAR(4)").charLength().where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();

        try {
            final List<String> list = employee.firstName.cast("CHAR(5)").map(CoreMappers.STRING).collate(validCollationNameForChar())
                    .where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // derby: does not support COLLATE
            System.out.println(getDatabaseName());
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.cast("CHAR(5)").concat(employee.lastName)
                .where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(Arrays.asList("JamesCooper"), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.cast("CHAR(5)").concat("=")
                .where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(Arrays.asList("James="), list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.concat(employee.firstName.cast("CHAR(5)"))
                .where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(Arrays.asList("CooperJames"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<String> list = employee.lastName.where(createCast(another).contains(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createCast(employee).countDistinct().list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createCast(employee).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(createCast(employee).desc(), employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood", "March", "Pedersen", "Cooper"), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).distinct().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).div(employee.salary.sub(1000))
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(5, list.size());
        assertEquals(Arrays.asList(3.0, 1.5, 2.0, 2.0, 1.5), list);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).div(1000)
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(5, list.size());
        assertEquals(Arrays.asList(1.5, 3.0, 2.0, 2.0, 3.0), list);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = Params.p(6000.0).div(createCast(employee))
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(5, list.size());
        assertEquals(Arrays.asList(4.0, 2.0, 3.0, 3.0, 2.0), list);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).eq(3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).eq(employee.salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.eq(createCast(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = department.manager().salary.exceptAll(createCast(employee)).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = createCast(employee).exceptAll(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = department.manager().salary.exceptDistinct(createCast(employee)).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = createCast(employee).exceptDistinct(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = department.manager().salary.except(createCast(employee)).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = createCast(employee).except(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Integer> list = department.deptName.count().where(createCast(employee).exists()).list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).ge(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).ge(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE).ge(createCast(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).gt(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_gt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).gt(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE).gt(createCast(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee sample = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).in(sample.salary.where(sample.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).in(1500.0, 3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.salary.in(department.manager().salary.cast("DECIMAL(6,2)")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = createCast(employee).intersectAll(department.manager().salary).list(getEngine());
            assertEquals(Arrays.asList(3000.0, 3000.0), list);
        } catch (SQLException e) {
            // MySQL does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = department.manager().salary.intersectAll(createCast(employee)).list(getEngine());
            assertEquals(Arrays.asList(3000.0, 3000.0), list);
        } catch (SQLException e) {
            // MySQL does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = createCast(employee).intersectDistinct(department.manager().salary).list(getEngine());
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // MySQL does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = department.manager().salary.intersectDistinct(createCast(employee)).list(getEngine());
            assertEquals(Arrays.asList(3000.00), list);
        } catch (SQLException e) {
            // MySQL does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = createCast(employee).intersect(department.manager().salary).list(getEngine());
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // MySQL does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final List<Double> list = department.manager().salary.intersect(createCast(employee)).list(getEngine());
            assertEquals(Arrays.asList(3000.00), list);
        } catch (SQLException e) {
            // MySQL does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.deptId.cast("DECIMAL(4)").isNotNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.cast("DECIMAL(4)").isNull()).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = createCast(employee).label(l).orderBy(l).list(getEngine());
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);

    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).le(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_le_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).le(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE).le(createCast(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);

    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.lastName.cast("CHAR(10)").like("M%")).list(getEngine());
        assertEquals(Arrays.asList("March"), list);
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(
                employee.lastName.cast("CHAR(5)").like(employee.lastName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).limit(3, 8).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).lt(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_lt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).lt(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE).lt(createCast(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createCast(employee).map(Mappers.INTEGER).orderBy(employee.salary).list(getEngine());
        assertEquals(Arrays.asList(1500, 2000, 2000, 3000, 3000), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).max().list(getEngine());
        assertEquals(Arrays.asList(3000.0), list);
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).min().list(getEngine());
        assertEquals(Arrays.asList(1500.0), list);
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).mult(employee.salary.div(1000))
                .map(Mappers.DOUBLE)
                .orderBy(employee.salary)
                .list(getEngine());
        assertEquals(Arrays.asList(2250.0, 4000.0, 4000.0, 9000.0, 9000.0), list);
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).mult(2)
                .map(Mappers.DOUBLE)
                .orderBy(employee.salary)
                .list(getEngine());
        assertEquals(Arrays.asList(3000.0, 4000.0, 4000.0, 6000.0, 6000.0), list);
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.div(1000).mult(createCast(employee))
                .map(Mappers.DOUBLE)
                .orderBy(employee.salary)
                .list(getEngine());
        assertEquals(Arrays.asList(2250.0, 4000.0, 4000.0, 9000.0, 9000.0), list);
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).ne(3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_ne_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).ne(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.mult(2).sub(2000).map(Mappers.DOUBLE).ne(createCast(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee sample = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).notIn(sample.salary.where(sample.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final Employee sample = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).notIn(1500.0, 3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.salary.notIn(department.manager().salary.cast("DECIMAL(6,2)")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.lastName.cast("CHAR(10)").notLike("M%"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.lastName.cast("CHAR(10)").notLike(employee.department().manager().lastName.concat("%")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
   }

    @Override
    public void test_nullsFirst_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.cast("BIGINT").nullsFirst()).list(getEngine());
            assertEquals(5, list.size());
            assertEquals("Cooper", list.get(0));
        } catch (SQLException e) {
            // mysql;: does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.cast("BIGINT").nullsLast()).list(getEngine());
            assertEquals(5, list.size());
            assertEquals("Cooper", list.get(4));
        } catch (SQLException e) {
            // mysql;: does not support NULLS LAST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).opposite().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.gt(2000.0)
                .then(employee.salary.mult(2).map(Mappers.DOUBLE))
                .orElse(createCast(employee))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 6000.0,6000.0), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createCast(employee), employee.lastName).list(getEngine());
        // Cooper, March, Pedersen, First, Redwood
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).orderBy(employee.lastName).list(getEngine());
        // Cooper, First, March, Pedersen, Redwood
        assertEquals(Arrays.asList(1500.0, 3000.0, 2000.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = createCast(employee).pair(employee.lastName)
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make(3000.0, "First"), list.get(0));
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Double>> list = employee.lastName.pair(createCast(employee))
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("First", 3000.0), list.get(0));
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractCastSpecification<Double> cast = createCast(employee);
        final DynamicParameter<Double> param = cast.param();
        param.setValue(2.0);
        final List<Number> list = cast.mult(param).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractCastSpecification<Double> cast = createCast(employee);
        final DynamicParameter<Double> param = cast.param(2.0);
        final List<Number> list = cast.mult(param).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_positionOf_String() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.cast("CHAR(5)").positionOf("rs")
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = employee.lastName.cast("CHAR(5)").positionOf(employee.lastName.substring(3))
                    .where(employee.lastName.eq("First"))
                    .list(getEngine());
            assertEquals(Arrays.asList(3), list);
        } catch (SQLException e) {
            // a bug in Apache Derby
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.cast("CHAR(5)").positionOf(employee.lastName.cast("CHAR(5)"))
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final Employee employee = new Employee();
        final MyDual myDual = new MyDual();
        final List<Pair<String, String>> list = employee.lastName.pair(myDual.dummy.cast("CHAR(1)").queryValue())
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("First", "X"), list.get(0));
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> expected = new ArrayList<>(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0));
        final int iterations = createCast(employee).scroll(getEngine(), new Callback<Double>() {
            @Override
            public boolean iterate(final Double aDouble) throws SQLException {
                assertTrue(expected.toString(), expected.remove(aDouble));
                return true;
            }
        });
        assertEquals(5, iterations);

    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).selectAll().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("sample"))).execute(getEngine());
            final AbstractUpdateStatementBase update = insertTable.update(insertTable.text.set(insertTable.text.cast("CHAR(3)")));
            System.out.println(update.showUpdate(new GenericDialect()));
            update.execute(getEngine());
            assertEquals(Arrays.asList("sam"), insertTable.text.list(getEngine()));
        } catch (SQLException e) {
            // com.mysql.jdbc.MysqlDataTruncation: Data truncation: Truncated incorrect CHAR(3) value: 'sample'
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createCast(employee).showQuery(new GenericDialect());
        Pattern pattern = Pattern.compile("SELECT CAST\\(([A-Z][A-Z0-9]*)\\.salary AS DECIMAL\\(6,2\\)\\) AS [A-Z][A-Z0-9]* FROM employee AS \\1");
        assertTrue(sql, pattern.matcher(sql).matches());

    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).sub(1000.0).map(Mappers.DOUBLE).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(500.0, 1000.0, 1000.0, 2000.0, 2000.0), list);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = Params.p(4000.0).sub(createCast(employee)).map(Mappers.DOUBLE).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1000.0, 1000.0, 2000.0, 2000.0, 2500.0), list);
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).sub(employee.salary.mult(2)).map(Mappers.DOUBLE).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.cast("CHAR(5)").substring(employee.lastName.positionOf("rs"))
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("rst"), list);
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.cast("CHAR(5)")
                .substring(employee.lastName.positionOf("rs"), employee.lastName.positionOf("rs"))
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("rst"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractCastSpecification<Number> offset;
        if (getDatabaseName().equals("MySQL")) {
            offset = employee.salary.div(1000).cast("DECIMAL(3)");
        } else {
            offset = employee.salary.div(1000).cast("INTEGER");
        }
        final List<String> list = employee.lastName.cast("CHAR(5)").substring(offset)
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("rst"), list);

    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractCastSpecification<Number> offset;
        if (getDatabaseName().equals("MySQL")) {
            offset = employee.salary.div(1000).cast("DECIMAL(3)");
        } else {
            offset = employee.salary.div(1000).cast("INTEGER");
        }
        final List<String> list = employee.lastName.cast("CHAR(5)").substring(offset, Params.p(2))
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("rs"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        final Employee employee = new Employee();
        final AbstractCastSpecification<Number> limit;
        if (getDatabaseName().equals("MySQL")) {
            limit = employee.salary.div(1000).cast("DECIMAL(3)");
        } else {
            limit = employee.salary.div(1000).cast("INTEGER");
        }
        final List<String> list = employee.lastName.cast("CHAR(5)").substring(Params.p(1), limit)
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("Fir"), list);
    }

    @Override
    public void test_substring_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.cast("CHAR(5)").substring(3)
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("rst"), list);
    }

    @Override
    public void test_substring_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.cast("CHAR(5)").substring(3, 2)
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("rs"), list);
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createCast(employee).sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(11500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = employee.salary.gt(2000.0).then(employee.lastName.cast("CHAR(3)")).orElse(employee.lastName);
        System.out.println(whenClauseList.showQuery(getEngine().getDialect()));
        final List<String> list = whenClauseList
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "Fir", "March", "Pedersen", "Red"), list);

    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = department.manager().salary.unionAll(createCast(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).unionAll(department.manager().salary).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = department.manager().salary.unionDistinct(createCast(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).unionDistinct(department.manager().salary).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = department.manager().salary.union(createCast(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).union(department.manager().salary).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.cast("CHAR(3)").where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList("Fir"), list);
    }

}
