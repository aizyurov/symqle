create table country (country_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name VARCHAR(100), code varchar(3))

create table department (dept_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, dept_name VARCHAR(100) NOT NULL, manager_id INTEGER, parent_dept INTEGER, country_id INTEGER)

create table employee (emp_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, first_name VARCHAR(30), last_name VARCHAR(50), title VARCHAR(50),
  hire_date DATE, is_retired BOOLEAN default false, salary FLOAT, dept_id INTEGER)

alter table department add foreign key (parent_dept) references department

alter table department add foreign key (country_id) references country

alter table department add foreign key (manager_id) references employee

alter table employee add foreign key (dept_id) references department


insert into country (name, code) values ('United States of America', 'USA')

insert into country (name, code) values ('Russian Federation', 'RUS')

insert into country (name, code) values ('France', 'FRA')


insert into department (dept_name, country_id) select 'HR', country_id from country c where c.code = 'USA'

insert into department (dept_name, country_id) select 'DEV', country_id from country c where c.code = 'RUS'

insert into employee (first_name, last_name, title, hire_date, salary, dept_id) select 'Margaret', 'Redwood',
 'HR Manager', '2008-08-22', 3000, dept_id
  from department d where d.dept_name = 'HR'

insert into employee (first_name, last_name, title, hire_date, salary, dept_id) select 'Bill', 'March',
 'HR Clerk', '2008-08-23', 2000, dept_id
  from department d where d.dept_name = 'HR'

update department set manager_id = (select emp_id from employee where last_name='Redwood') where dept_name = 'HR'

insert into employee (first_name, last_name, title, hire_date, salary, dept_id) select 'James', 'First',
 'Development manager', '2008-10-01', 3000, dept_id
  from department d where d.dept_name = 'DEV'

update department set manager_id = (select emp_id from employee where last_name='First') where dept_name = 'DEV'

insert into employee (first_name, last_name, title, hire_date, salary, dept_id) select 'Alex', 'Pedersen',
 'guru', '2008-10-11', 2000, dept_id
  from department d where d.dept_name = 'DEV'

insert into employee (first_name, last_name, title, hire_date, salary, dept_id, is_retired) values ('James', 'Cooper',
 'hacker', '2009-01-12', 1500, null , true)

create table big_table (num integer)

create table my_dual (dummy char(1))

insert into my_dual (dummy) values ('X')

create table delete_master (master_id integer primary key, description varchar(30))

create table delete_detail (detail_id integer primary key, master_id integer, detail varchar(30))

alter table delete_detail add foreign key (master_id) references delete_master

create table one (id integer)

insert into one (id) values (1)

create table insert_test (id integer default 0, text varchar(50) default 'nothing')

create table true_value (value boolean)

insert into true_value values(true)

create table update_test (id integer default 0, text varchar(50) default 'nothing')

create table join_test_left (id integer, text varchar(50))

create table join_test_right (id integer, text varchar(50))

insert into join_test_left (id, text) values (1, 'one')

insert into join_test_left (id, text) values (2, 'two')

insert into join_test_right (id, text) values (2, 'two')

insert into join_test_right (id, text) values (3, 'three')

create table generated_keys (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, text varchar(50))

insert into generated_keys (text) values ('zero')

create table arithmetics (leftInt integer, rightInt integer, leftDouble double, rightDouble double)

insert into arithmetics (leftInt, rightInt, leftDouble, rightDouble) values (7, 3, 11.0, 2.0)

create table all_types (
  t_BIT bigint,
  t_TINYINT smallint,
  t_SMALLINT smallint,
  t_MEDIUMINT integer,
  t_INTEGER integer,
  t_BIGINT bigint,
  t_FLOAT float(23),
  t_REAL real,
  t_DOUBLE double,
  t_NUMERIC numeric(10,3),
  t_DECIMAL decimal(10,3),
  t_CHAR char(10),
  t_VARCHAR varchar(20),
  t_LONGVARCHAR long varchar,
  t_DATE date,
  t_TIME time,
  t_TIMESTAMP timestamp,
  t_DATETIME timestamp,
  t_BINARY CHAR (4) FOR BIT DATA,
  t_VARBINARY VARCHAR (1000) FOR BIT DATA,
  t_LONGVARBINARY LONG VARCHAR FOR BIT DATA,
  t_BLOB blob,
  t_CLOB clob,
  t_BOOLEAN boolean,
  t_NCHAR char(10),
  t_NVARCHAR varchar(20),
  t_LONGNVARCHAR long varchar,
  t_NCLOB clob
)

