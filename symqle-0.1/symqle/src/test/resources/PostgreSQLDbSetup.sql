drop table if exists employee cascade

drop table if exists  department cascade

drop table if exists  country cascade

drop table if exists big_table, my_dual cascade

drop table if exists delete_detail cascade

drop table if exists delete_master cascade

drop table if exists one, insert_test, true_value, join_test_left, join_test_right, arithmetics, generated_keys, update_test

create table country (country_id INTEGER PRIMARY KEY, name VARCHAR(100), code varchar(3))

create table department (dept_id INTEGER PRIMARY KEY, dept_name VARCHAR(100) NOT NULL, manager_id INTEGER, parent_dept INTEGER, country_id INTEGER)

create table employee (emp_id INTEGER PRIMARY KEY, first_name VARCHAR(30), last_name VARCHAR(50), title VARCHAR(50),
  hire_date DATE, is_retired BOOLEAN default false, salary FLOAT, dept_id INTEGER)

alter table department add foreign key (parent_dept) references department

alter table department add foreign key (country_id) references country

alter table department add foreign key (manager_id) references employee

alter table employee add foreign key (dept_id) references department


insert into country (country_id, name, code) values (1, 'United States of America', 'USA')

insert into country (country_id, name, code) values (2, 'Russian Federation', 'RUS')

insert into country (country_id, name, code) values (3, 'France', 'FRA')


insert into department (dept_id, dept_name, country_id) select 1, 'HR', country_id from country c where c.code = 'USA'

insert into department (dept_id, dept_name, country_id) select 2, 'DEV', country_id from country c where c.code = 'RUS'

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) select 1, 'Margaret', 'Redwood',
 'HR Manager', '2008-08-22', 3000, dept_id
  from department d where d.dept_name = 'HR'

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) select 2, 'Bill', 'March',
 'HR Clerk', '2008-08-23', 2000, dept_id
  from department d where d.dept_name = 'HR'

update department set manager_id = (select emp_id from employee where last_name='Redwood') where dept_name = 'HR'

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) select 3, 'James', 'First',
 'Development manager', '2008-10-01', 3000, dept_id
  from department d where d.dept_name = 'DEV'

update department set manager_id = (select emp_id from employee where last_name='First') where dept_name = 'DEV'

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) select 4, 'Alex', 'Pedersen',
 'guru', '2008-10-11', 2000, dept_id
  from department d where d.dept_name = 'DEV'

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id, is_retired) values (5, 'James', 'Cooper',
 'hacker', '2009-01-12', 1500, null , true)

create table big_table (num integer)

create table my_dual (dummy char(1))

insert into my_dual (dummy) values ('X')

create table delete_master (master_id integer primary key, description varchar(30))

create table delete_detail (detail_id integer primary key, master_id integer, detail varchar(30))

alter table delete_detail add foreign key (master_id) references delete_master

create table one (id integer)

insert into one (id) values (1)

create table insert_test (id integer default 0, text varchar(50) default 'nothing', active boolean default true, payload integer)

create table true_value (value boolean)

insert into true_value values(true)

create table update_test (id integer default 0, text varchar(50) default 'nothing')

create table join_test_left (id integer, text varchar(50))

create table join_test_right (id integer, text varchar(50))

insert into join_test_left (id, text) values (1, 'one')

insert into join_test_left (id, text) values (2, 'two')

insert into join_test_right (id, text) values (2, 'two')

insert into join_test_right (id, text) values (3, 'three')

drop sequence if exists generated_keys_seq

create sequence generated_keys_seq

create table generated_keys (id integer default nextval('generated_keys_seq'), text varchar(50))

insert into generated_keys (text) values ('zero')

create table arithmetics (leftInt integer, rightInt integer, leftDouble double precision, rightDouble double precision)

insert into arithmetics (leftInt, rightInt, leftDouble, rightDouble) values (7, 3, 11.0, 2.0)

drop table if exists all_types

create table all_types (t_BIT bigint,
  t_TINYINT smallint,
  t_SMALLINT smallint,
  t_MEDIUMINT integer,
  t_INTEGER integer,
  t_BIGINT bigint,
  t_FLOAT real,
  t_REAL real,
  t_DOUBLE double precision,
  t_NUMERIC numeric(10,3),
  t_DECIMAL numeric(10,3),
  t_CHAR char(10),
  t_VARCHAR varchar(20),
  t_LONGVARCHAR text,
  t_DATE date,
  t_TIME time,
  t_TIMESTAMP timestamp,
  t_DATETIME timestamp,
  t_BINARY bytea,
  t_VARBINARY bytea,
  t_LONGVARBINARY bytea,
  t_BLOB bytea,
  t_CLOB text,
  t_BOOLEAN boolean,
  t_NCHAR char(10),
  t_NVARCHAR varchar(20),
  t_LONGNVARCHAR text,
  t_NCLOB text
)
