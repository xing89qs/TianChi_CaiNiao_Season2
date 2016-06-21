package my.group.GradientBoostingRegression;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class FeatureMapper implements Mapper {
	private final int ENSEMBLE_MODEL_NUM = 10;
	private Record key;
	private Record value;

	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
		value = context.createMapOutputValueRecord();
	}

	private String[] features = { "qty_alipay_njhs_sum", "qty_alipay_njhs_min", "qty_alipay_njhs_stddev",
			"qty_alipay_njhs_avg", "qty_alipay_njhs_median", "pv_ipv_avg", "pv_uv_avg", "cart_ipv_avg", "cart_uv_avg",
			"collect_uv_avg", "brand_qty_alipay_njhs_avg", "cate_qty_alipay_njhs_avg", "cate_level_qty_alipay_njhs_avg",
			"amt_alipay_njhs_sum", "unum_alipay_njhs_avg", "pv_uv_avg", "cart_uv_avg" };
	private String[] single_features = { "_total_sell", "_first_sell", "_last_sell" };
	private int[] days = { 1, 2, 3, 4, 5, 6, 7, 14, 21, 30, 60, 365, 366 };

	public void map(long key, Record record, TaskContext context) throws IOException {
		value.setBigint("flag", 0L);
		if (context.getInputTableInfo().getTableName().startsWith("tree_table")) {
			if (record.getBigint("item_id") != -2L)
				return;
			value.setBigint("is_tree", 1L);
			value.setBigint("root_id", record.getBigint("root_id"));
			value.setBigint("left_son", record.getBigint("left_son"));
			value.setBigint("right_son", record.getBigint("right_son"));
			value.setBigint("split_feature", record.getBigint("split_feature"));
			value.setDouble("split_feature_value", record.getDouble("split_feature_value"));
			value.setBigint("estimator_num", record.getBigint("estimator_num"));
			value.setBigint("is_root", record.getBigint("is_root"));
			value.setDouble("node_value", record.getDouble("node_value"));
			this.key.setBigint("val", record.getBigint("val"));
			context.write(this.key, value);
		} else {
			value.setBigint("is_tree", 0L);
			if (record.getBigint("item_id") == -1L)
				return;
			String store_code = record.getString("store_code");
			double targetA = record.getDouble("targeta");
			double targetB = record.getDouble("targetb");
			long val = 0;
			double alpha = 0;
			long max_depth = 5;
			int n_estimator = 150;

			if (store_code.equals("all")) {
				val = 0;
				alpha = 0;
				if (targetA >= targetB) {
					final double[] SPLIT_ARRAY = { 2.5 };
					final double[] ALPHA_ARRAY = { 0.77 };
					final int[] ESTIMATOR_ARRAY = { 180 };
					final int[] DEPTH_ARRAY = { 3 };
					final int[] VAL_ARRAY = { 0, };
					double times = targetA / targetB;
					for (int i = 0; i < SPLIT_ARRAY.length; i++) {
						if (times < SPLIT_ARRAY[i]) {
							val = VAL_ARRAY[i];
							alpha = ALPHA_ARRAY[i];
							max_depth = DEPTH_ARRAY[i];
							n_estimator = ESTIMATOR_ARRAY[i];
							break;
						}
					}
				} else {
					// final double[] SPLIT_ARRAY = { 1.27747, 1.67347, 2.5 };
					// final double[] ALPHA_ARRAY = { 0.5, 0.48, 0.44 };
					// final int[] DEPTH_ARRAY = { 4, 4, 4 };
					// final int[] ESTIMATOR_ARRAY = { 150, 160, 100 };
					// final int[] VAL_ARRAY = { 1, 2, 3, };
					final double[] SPLIT_ARRAY = { 2.5 };
					final double[] ALPHA_ARRAY = { 0.47 };
					final int[] DEPTH_ARRAY = { 4 };
					final int[] VAL_ARRAY = { 1 };

					double times = targetB / targetA;
					for (int i = 0; i < SPLIT_ARRAY.length; i++) {
						if (times < SPLIT_ARRAY[i]) {
							val = VAL_ARRAY[i];
							alpha = ALPHA_ARRAY[i];
							max_depth = DEPTH_ARRAY[i];
							n_estimator = 150;
							break;
						}
					}
				}
			} else {
				val = record.getBigint("append_id");
				final int BLOCK = 150000;
				val /= BLOCK;
				// final double[] ALPHA_ARRAY = { 0.45, 0.5, 0.54, 0.49, 0.54,
				// 0.54, 0.53, 0.51, 0.49, 0.47, 0.54, 0.53,
				// 0.54, 0.53, 0.51, 0.52, 0.5, 0.53, 0.5, 0.75, 0.76, 0.72,
				// 0.76, 0.74 };
				// final int[] DEPTH_ARRAY = { 3, 3, 3, 5, 5, 5, 5, 5, 5, 4, 4,
				// 4, 4, 3, 5, 4, 5, 4, 5, 4, 4, 3, 4, 4, };
				// final int[] ESTIMATOR_ARRAY = { 130, 120, 110, 200, 100, 130,
				// 100, 100, 100, 150, 150, 200, 200, 170,
				// 130, 180, 150, 130, 200, 150, 140, 200, 200, 150 };
				// alpha = ALPHA_ARRAY[(int) val];
				// max_depth = DEPTH_ARRAY[(int) val];
				// n_estimator = ESTIMATOR_ARRAY[(int) val];
				n_estimator = 180;
				max_depth = 6;
				if (targetA > targetB && val != 5) {
					alpha = 0.72 + (val - 5) * 0.05 / 3;
				} else {
					alpha = 0.45 + val * 0.07 / 5;
				}
				val += 2;
			}
			this.key.set("val", val);
			this.value.setDouble("alpha", alpha);
			this.value.setBigint("max_depth", max_depth);
			this.value.setBigint("n_estimator", n_estimator + 0L);
			for (int j = 0; j < days.length; j++) {
				for (int i = 0; i < features.length; i++) {
					String featureName = "_" + days[j] + "day_" + features[i];
					value.set(featureName, record.get(featureName));
				}
				FeatureGennerator.generateFeature(record, days[j], value);
			}
			FeatureGennerator.generateFeature(record, value);
			for (int i = 0; i < single_features.length; i++)
				value.set(single_features[i], record.get(single_features[i]));
			value.setDouble("targeta", targetA);
			value.setDouble("targetb", targetB);
			value.setBigint("item_id", record.getBigint("item_id"));
			value.setBigint("y", record.getBigint("y"));
			value.setString("store_code", record.getString("store_code"));
			value.setBigint("train", record.getBigint("train"));
			// for (int i = 0; i < 1; i++) {
			// this.key.setBigint("val", 28 * i + val);
			// this.value.setDouble("learning_rate", 0.1);
			// this.value.setDouble("sample_rate", 1.0);
			// this.value.setDouble("feature_rate", 1.0);
			// this.value.setDouble("sample_weight", targetA);
			// //this.value.setBigint("max_depth", 3L);
			// this.value.setBigint("flag", i + 0L);
			// context.write(this.key, value);
			// }
			for (int i = 0; i < ENSEMBLE_MODEL_NUM; i++) {
				this.key.setBigint("val", i * 10 + val);
				if (store_code.equals("all")) {
					this.value.setDouble("learning_rate", 0.08 + 0.02 * i);
					this.value.setDouble("sample_rate", 0.8);
					this.value.setDouble("feature_rate", 0.8);
				} else {
					this.value.setDouble("learning_rate", 0.1);
					this.value.setDouble("sample_rate", 0.8);
					this.value.setDouble("feature_rate", 0.8);
				}
				this.value.setDouble("sample_weight", targetA);
				context.write(this.key, value);
			}
			for (int i = 0; i < 4; i++) {
				this.key.setBigint("val", 10 * 10 + i * 10 + val);
				if (store_code.equals("all")) {
					this.value.setDouble("learning_rate", 0.1);
					this.value.setDouble("sample_rate", 0.8);
					this.value.setDouble("feature_rate", 0.8);
				} else {
					this.value.setDouble("learning_rate", 0.1);
					this.value.setDouble("sample_rate", 0.8);
					this.value.setDouble("feature_rate", 0.8);
				}
				this.value.setDouble("sample_weight", 1.0);
				context.write(this.key, value);
			}
		}

	}

	public void cleanup(TaskContext context) throws IOException {
		context.getJobConf().setLong("mapred.task.timeout", 0L);
	}
}

class FeatureGennerator {

	private static void cart_buy_rate(Record record, int day, Record value) {
		double qty_alipay_njhs = record.getBigint("_" + day + "day_qty_alipay_njhs_sum");
		double cart_ipv = record.getBigint("_" + day + "day_cart_ipv_sum");
		double cart_buy_rate = -1;
		if (cart_ipv > 0)
			cart_buy_rate = qty_alipay_njhs / cart_ipv;
		value.set("_" + day + "day_cart_buy_rate", cart_buy_rate);
	}

	private static void brow_buy_rate(Record record, int day, Record value) {
		double qty_alipay_njhs = record.getBigint("_" + day + "day_qty_alipay_njhs_sum");
		double pv_ipv = record.getBigint("_" + day + "day_pv_ipv_sum");
		double brow_buy_rate = -1;
		if (pv_ipv > 0)
			brow_buy_rate = qty_alipay_njhs / pv_ipv;
		value.set("_" + day + "day_brow_buy_rate", brow_buy_rate);
	}

	private static void collect_buy_rate(Record record, int day, Record value) {
		double qty_alipay_njhs = record.getBigint("_" + day + "day_qty_alipay_njhs_sum");
		double collect_uv = record.getBigint("_" + day + "day_collect_uv_sum");
		double collect_buy_rate = -1;
		if (collect_uv > 0)
			collect_buy_rate = qty_alipay_njhs / collect_uv;
		value.set("_" + day + "day_collect_buy_rate", collect_buy_rate);
	}

	private static void _4day_divide_14day_qty_alipay_njhs(Record record, Record value) {
		double _4day_qty_alipay_njhs_avg = record.getDouble("_" + 4 + "day_qty_alipay_njhs_avg");
		double _14day_qty_alipay_njhs_avg = record.getDouble("_" + 14 + "day_qty_alipay_njhs_avg");
		double feature = -1;
		if (_4day_qty_alipay_njhs_avg > 0)
			feature = _4day_qty_alipay_njhs_avg / _14day_qty_alipay_njhs_avg;
		value.setDouble("_4day_divide_14day_qty_alipay_njhs", feature);
		feature = Math.max(0, _4day_qty_alipay_njhs_avg) - Math.max(0, _14day_qty_alipay_njhs_avg);
		value.setDouble("_4day_substract_14day_qty_alipay_njhs", feature);
	}

	private static void _365day_divide_366day_qty_alipay_njhs(Record record, Record value) {
		double _365day_qty_alipay_njhs_avg = record.getDouble("_" + 365 + "day_qty_alipay_njhs_avg");
		double _366day_qty_alipay_njhs_avg = record.getDouble("_" + 366 + "day_qty_alipay_njhs_avg");
		double feature = -1;
		if (_365day_qty_alipay_njhs_avg > 0)
			feature = _365day_qty_alipay_njhs_avg / _366day_qty_alipay_njhs_avg;
		value.setDouble("_365day_divide_366day_qty_alipay_njhs", feature);
		feature = Math.max(0, _365day_qty_alipay_njhs_avg) - Math.max(0, _366day_qty_alipay_njhs_avg);
		value.setDouble("_365day_substract_366day_qty_alipay_njhs", feature);
	}

	private static void _item_new_and_many_brow(Record record, Record value) {
		double _14day_qty_alipay_njhs_sum = record.getBigint("_" + 14 + "day_qty_alipay_njhs_sum");
		double _14day_pv_ipv_sum = record.getBigint("_" + 14 + "day_pv_ipv_sum");
		if (_14day_qty_alipay_njhs_sum == 0 && _14day_pv_ipv_sum > 1000)
			value.setBigint("_item_new_and_many_brow", 1L);
		else
			value.setBigint("_item_new_and_many_brow", 0L);
	}

	private static void price_avg(Record record, int day, Record value) {
		double qty_alipay_njhs = record.getBigint("_" + day + "day_qty_alipay_njhs_sum");
		double amt_alipay_njhs = record.getDouble("_" + day + "day_amt_alipay_njhs_sum");
		double price_avg = -1;
		if (qty_alipay_njhs > 0)
			price_avg = amt_alipay_njhs / qty_alipay_njhs;
		value.setDouble("_" + day + "day_price_avg", price_avg);
	}

	private static void discount(Record record, int day, Record value) {
		double amt_gmv = record.getDouble("_" + day + "day_amt_gmv_sum");
		double amt_alipay_njhs = record.getDouble("_" + day + "day_amt_alipay_njhs_sum");
		double discount = -1;
		if (amt_gmv > 0)
			discount = amt_alipay_njhs / amt_gmv;
		value.setDouble("_" + day + "day_discount", discount);
	}

	private static void single_day(Record record, Record value) {
		long last = record.getBigint("_1day_qty_alipay_njhs_sum");
		if (last == -1L)
			last = 0;
		for (int day = 2; day <= 7; day++) {
			long qty_alipay_njhs = record.getBigint("_" + day + "day_qty_alipay_njhs_sum");
			long qty_alipay_njhs_1 = record.getBigint("_" + (day - 1) + "day_qty_alipay_njhs_sum");
			if (qty_alipay_njhs == -1L)
				qty_alipay_njhs = 0;
			if (qty_alipay_njhs_1 == -1L)
				qty_alipay_njhs_1 = 0;
			value.setBigint("_single_" + day + "day_qty_alipay_njhs", qty_alipay_njhs - qty_alipay_njhs_1 + 0L);
			value.setBigint("_single_" + day + "day_substract_qty_alipay_njhs",
					qty_alipay_njhs - qty_alipay_njhs_1 - last);
		}
	}

	public static void generateFeature(Record record, int day, Record value) {
		cart_buy_rate(record, day, value);
		price_avg(record, day, value);
		discount(record, day, value);
		collect_buy_rate(record, day, value);
		brow_buy_rate(record, day, value);
		substract_feature(record, day, value);
	}

	private static void substract_feature(Record record, int day, Record value) {
		// TODO Auto-generated method stub
		double qty_alipay_njhs_avg = Math.max(0.0, record.getDouble("_" + day + "day_qty_alipay_njhs_avg"));
		double cate_qty_alipay_njhs_avg = Math.max(0.0, record.getDouble("_" + day + "day_cate_qty_alipay_njhs_avg"));
		double brand_qty_alipay_njhs_avg = Math.max(0.0, record.getDouble("_" + day + "day_brand_qty_alipay_njhs_avg"));
		double cate_level_qty_alipay_njhs_avg = Math.max(0.0,
				record.getDouble("_" + day + "day_cate_level_qty_alipay_njhs_avg"));
		double supplier_qty_alipay_njhs_avg = Math.max(0.0,
				record.getDouble("_" + day + "day_supplier_qty_alipay_njhs_avg"));
		value.setDouble("_" + day + "day_cate_qty_alipay_njhs_substract_avg",
				qty_alipay_njhs_avg - cate_qty_alipay_njhs_avg);
		value.setDouble("_" + day + "day_cate_level_qty_alipay_njhs_substract_avg",
				qty_alipay_njhs_avg - cate_level_qty_alipay_njhs_avg);
		value.setDouble("_" + day + "day_brand_qty_alipay_njhs_substract_avg",
				qty_alipay_njhs_avg - brand_qty_alipay_njhs_avg);
		value.setDouble("_" + day + "day_supplier_qty_alipay_njhs_substract_avg",
				qty_alipay_njhs_avg - supplier_qty_alipay_njhs_avg);
	}

	public static void generateFeature(Record record, Record value) {
		_4day_divide_14day_qty_alipay_njhs(record, value);
		_365day_divide_366day_qty_alipay_njhs(record, value);
		_item_new_and_many_brow(record, value);
		single_day(record, value);
		// cate_level_id(record, value);
	}

	private static void cate_level_id(Record record, Record value) {
		// TODO Auto-generated method stub
		for (int i = 1; i <= 13; i++) {
			value.setBigint("_is_cate_level_id_" + i, record.getBigint("cate_level_id") == i ? 1L : 0L);
		}
	}
}