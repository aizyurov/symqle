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

insert into employee (first_name, last_name, title, hire_date, salary, dept_id, is_retired) select 'James', 'Cooper',
 'hacker', '2009-01-12', 1500, dept_id, true
  from department d where d.dept_name = 'DEV'










