drop table if exists employee, department, country

create table country (country_id INTEGER PRIMARY KEY, name VARCHAR(100), code varchar(3))

create table department (dept_id INTEGER PRIMARY KEY, dept_name VARCHAR(100) NOT NULL, manager_id INTEGER, parent_dept INTEGER, country_id INTEGER)

create table employee (emp_id INTEGER PRIMARY KEY, first_name VARCHAR(30), last_name VARCHAR(50), title VARCHAR(50),
  hire_date DATE, is_retired BOOLEAN default false, salary FLOAT, dept_id INTEGER)

alter table department add foreign key (parent_dept) references department (dept_id)

alter table department add foreign key (country_id) references country (country_id)

alter table department add foreign key (manager_id) references employee (emp_id)

alter table employee add foreign key (dept_id) references department (dept_id)


insert into country (country_id, name, code) values (1, 'United States of America', 'USA')

insert into country (country_id, name, code) values (2, 'Russian Federation', 'RUS')

insert into country (name, code) values ('France', 'FRA')

insert into department (dept_id, dept_name, country_id) values(1, 'HR', 1)

insert into department (dept_id, dept_name, country_id) values (2, 'DEV', 2)

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) values (101, 'Margaret', 'Redwood',
 'HR Manager', '2008-08-22', 3000, 1 )

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) values (102, 'Bill', 'March',
 'HR Clerk', '2008-08-23', 2000, 1 )

update department set manager_id = 101 where dept_name = 'HR'

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) values (201, 'James', 'First',
 'Development manager', '2008-10-01', 3000, 2 )

update department set manager_id = 201 where dept_name = 'DEV'

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id) values (202, 'Alex', 'Pedersen',
 'guru', '2008-10-11', 2000, 2 )

insert into employee (emp_id, first_name, last_name, title, hire_date, salary, dept_id, is_retired) values (203, 'James', 'Cooper',
 'hacker', '2009-01-12', 1500, null, true )

create table big_table (num integer)

create table my_dual (dummy char(1))

insert into my_dual (dummy) values ('X')

create table delete_master (master_id integer primary key, description varchar(30))

create table delete_detail (detail_id integer primary key, master_id integer, detail varchar(30))

alter table delete_detail add foreign key (master_id) references delete_master(master_id)







