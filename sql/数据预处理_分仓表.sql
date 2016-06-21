--odps sql 
--********************************************************************--
--author:xing89qs
--create time:2016-05-19 19:29:45
--********************************************************************--

-- item_feature表过滤双11、双12


select r.*, thedate from record_frame r;

select thedate from record_frame;

-- 分成3部分拼起来
-- 12.12之后
drop table if exists _record_frame1;
create table _record_frame1 as select 
		r.thedate as record_date, r.*
from record_frame r where thedate>20151212;

-- 11.11到11.12
drop table if exists _record_frame2;
create table _record_frame2 as select 
		cast(
			to_char(
				dateadd(
					concat(
					  	substr(to_char(thedate),1,4),'-',
						substr(to_char(thedate),5,2),'-',
					 	substr(to_char(thedate),7,2), ' 00:00:00'
					),
					1, 
					'dd'
				),
				'yyyymmdd'
			)
			as bigint
		) as record_date, 
		r.*
from record_frame r where thedate<20151212 and thedate>20151111;

-- 2014.12.12到2015.11.10
drop table if exists _record_frame3;
create table _record_frame3 as select 
		cast(
			to_char(
				dateadd(
					concat(
					  	substr(to_char(thedate),1,4),'-',
						substr(to_char(thedate),5,2),'-',
					 	substr(to_char(thedate),7,2), ' 00:00:00'
					),
					2, 
					'dd'
				),
				'yyyymmdd'
			)
			as bigint
		) as record_date, 
		r.*
from record_frame r where thedate>20141212 and thedate<20151111;

-- 2014.12.12到2015.11.10
drop table if exists _record_frame3;
create table _record_frame3 as select 
		cast(
			to_char(
				dateadd(
					concat(
					  	substr(to_char(thedate),1,4),'-',
						substr(to_char(thedate),5,2),'-',
					 	substr(to_char(thedate),7,2), ' 00:00:00'
					),
					3, 
					'dd'
				),
				'yyyymmdd'
			)
			as bigint
		) as record_date, 
		r.*
from record_frame r where thedate>20141212 and thedate<20151111;

-- 2014.12.12以前
drop table if exists _record_frame4;
create table _record_frame4 as select 
		cast(
			to_char(
				dateadd(
					concat(
					  	substr(to_char(thedate),1,4),'-',
						substr(to_char(thedate),5,2),'-',
					 	substr(to_char(thedate),7,2), ' 00:00:00'
					),
					4, 
					'dd'
				),
				'yyyymmdd'
			)
			as bigint
		) as record_date, 
		r.*
from record_frame r where thedate<20141212;

drop table if exists record_frame;

create table record_frame as
	select * from (
		select *from _record_frame1
			union all
		select *from _record_frame2
			union all
		select *from _record_frame3
			union all
		select *from _record_frame4
	) t;
