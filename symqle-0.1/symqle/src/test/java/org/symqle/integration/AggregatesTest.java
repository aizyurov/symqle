package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractAggregateFunction;
import org.symqle.sql.AggregateFunction;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractAggregateFunctionTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class AggregatesTest extends AbstractIntegrationTestBase  implements AbstractAggregateFunctionTestSet {

    @Override
    public void test_adapt_AggregateFunction() throws Exception {
        final Employee employee = new Employee();
        final AggregateFunction<Integer> count = employee.empId.count();
        final List<Integer> list = AbstractAggregateFunction.adapt(count).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().compileQuery(getEngine()).list();
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName.where(employee.empId.count().contains(5)).
                orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().distinct().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(100.0)).
                    exceptAll(employee.empId.count()).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().
                    exceptAll(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(100.0)).
                    exceptDistinct(employee.empId.count()).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().
                    exceptDistinct(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(100.0)).
                    except(employee.empId.count()).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().
                    except(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().forReadOnly().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
            // it is unclear, what 'for update' means when selecting count. Lock the whole table? Nevertheless, some engines allow it.
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().forUpdate().list(getEngine());
            assertEquals(Arrays.asList(5), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with aggregate functions
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        final List<String> list = employee.lastName.where(employee.empId.in(allEmployees.deptId.count())).list(getEngine());
        assertEquals(Arrays.asList("Pedersen"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().
                    intersectAll(employee.empId.count().where(employee.salary.lt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.lt(1800.0)).
                    intersectAll(employee.empId.count()).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().
                    intersectDistinct(employee.empId.count().where(employee.salary.lt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.lt(1800.0)).
                    intersectDistinct(employee.empId.count()).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().
                    intersect(employee.empId.count().where(employee.salary.gt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).
                    intersect(employee.empId.count()).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().limit(1).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().limit(1, 1).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        final List<String> list = employee.lastName.where(employee.empId.notIn(allEmployees.deptId.count())).
                orderBy(employee.lastName).
                list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Redwood"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final AbstractAggregateFunction<Integer> count = employee.empId.count();
        final List<Integer> list = count.orderBy(employee.empId).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_selectAll_() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    public void testSelect() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().union(employee.empId.count().where(employee.salary.gt(1800.0))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(4, 5), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().unionAll(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
        assertEquals(Arrays.asList(5, 5), list);
    }
    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().unionDistinct(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().except(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().exceptAll(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().exceptDistinct(employee.empId.count().where(employee.salary.gt(100.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().intersect(employee.empId.count().where(employee.salary.gt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().intersectAll(employee.empId.count().where(employee.salary.lt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().intersectDistinct(employee.empId.count().where(employee.salary.lt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }


    public void testForUpdate() throws Exception {
            // it is unclear, what 'for update' means when selecting count. Lock the whole table? Nevertheless, some engines allow it.
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().forUpdate().list(getEngine());
            assertEquals(Arrays.asList(5), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with aggregate functions
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().forReadOnly().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    public void testExists() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testContains() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().contains(11500.0))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testIn() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(5).in(employee.empId.count()))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testQueryValue() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Pair<String,Integer>> list = department.deptName
                .pair(
                        employee.empId.count().queryValue()
                ).orderBy(department.deptName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("DEV", 5), Pair.make("HR", 5)), list);
    }


}
