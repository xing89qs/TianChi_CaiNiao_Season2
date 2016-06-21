--odps sql 
--********************************************************************--
--author:xing89qs
--create time:2016-05-20 14:51:16
--********************************************************************--

drop table if exists config;
create table if not exists config as
	select *from tianchi_data.p2_config;

drop table if exists record_frame;
create table if not exists record_frame as select *from tianchi_data.p2_item_feature;

drop table if exists record_store_frame;
create table if not exists record_store_frame as select *from tianchi_data.p2_item_store_feature;