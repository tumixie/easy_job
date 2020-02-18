select * from test.loan_all limit {limit1};

drop table if exists test.test2;
create table test.test2 as
select * from test.loan_all a
limit {limit2};
