--odps sql 
--********************************************************************--
--author:xing89qs
--create time:2016-05-19 23:00:50
--********************************************************************--



drop table if exists record_table;

create table record_table as 
	select * from (
		select 'all' as store_code, to_date(to_char(record_date),'yyyymmdd') as _date, record_date,
			item_id, cate_id, cate_level_id, brand_id, supplier_id, pv_ipv, pv_uv,
			cart_ipv, cart_uv, collect_uv, num_gmv, amt_gmv, qty_gmv, unum_gmv, amt_alipay,
			num_alipay, qty_alipay, unum_alipay, ztc_pv_ipv, tbk_pv_ipv, ss_pv_ipv, jhs_pv_ipv,
			ztc_pv_uv, tbk_pv_uv, ss_pv_uv, jhs_pv_uv, num_alipay_njhs, amt_alipay_njhs, qty_alipay_njhs,
			unum_alipay_njhs
		from record_frame
			union all
		select to_char(store_code) as store_code, to_date(to_char(record_date),'yyyymmdd') as _date, record_date,
			item_id, cate_id, cate_level_id, brand_id, supplier_id, pv_ipv, pv_uv,
			cart_ipv, cart_uv, collect_uv, num_gmv, amt_gmv, qty_gmv, unum_gmv, amt_alipay,
			num_alipay, qty_alipay, unum_alipay, ztc_pv_ipv, tbk_pv_ipv, ss_pv_ipv, jhs_pv_ipv,
			ztc_pv_uv, tbk_pv_uv, ss_pv_uv, jhs_pv_uv, num_alipay_njhs, amt_alipay_njhs, qty_alipay_njhs,
			unum_alipay_njhs
		from record_store_frame
	) t;