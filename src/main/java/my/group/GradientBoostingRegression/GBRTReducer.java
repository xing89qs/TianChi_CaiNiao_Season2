package my.group.GradientBoostingRegression;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import my.group.GradientBoostingRegression.gbrt.GradientBoostingRegressor;
import my.group.GradientBoostingRegression.gbrt.GradientBoostingRegressor.TreeInfo;
import my.group.GradientBoostingRegression.gbrt.Sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class GBRTReducer implements Reducer {
	private Record result;

	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	static class DataSet {
		long item_id;
		String store_code;
		Sample X;
		double Y, sample_weight;
		double targeta, targetb;
	}

	private String[] all_features = { "qty_alipay_njhs_min", "qty_alipay_njhs_stddev", "qty_alipay_njhs_avg",
			"qty_alipay_njhs_median", "pv_ipv_avg", "cart_ipv_avg", "collect_uv_avg", "price_avg", "discount",
			"cate_qty_alipay_njhs_avg", "cate_level_qty_alipay_njhs_avg", "brand_qty_alipay_njhs_avg" };
	private String[] fencang_features = { "qty_alipay_njhs_min", "qty_alipay_njhs_stddev", "qty_alipay_njhs_avg",
			"qty_alipay_njhs_median", "pv_ipv_avg", "cart_ipv_avg", "collect_uv_avg", "discount", "price_avg",
			"cate_qty_alipay_njhs_avg", "cate_level_qty_alipay_njhs_avg", "brand_qty_alipay_njhs_avg" };
	private String[] all_single_features = { "_total_sell", "_first_sell", "_last_sell",
			"_4day_divide_14day_qty_alipay_njhs", };
	private String[] fencang_single_features = { "_total_sell", "_first_sell", "_last_sell",
			"_4day_divide_14day_qty_alipay_njhs", "_item_new_and_many_brow", };
	private int[] days_all = { 1, 2, 4, 7, 14, 30 };
	private int[] days_fencang = { 1, 2, 4, 7, 14, 21, 30 };

	private ArrayList<String> getFencangFeatures(Record key, Record record) {
		ArrayList<String> featureList = new ArrayList<String>();
		for (int j = 0; j < days_fencang.length; j++) {
			for (int i = 0; i < fencang_features.length; i++) {
				String featureName = "_" + days_fencang[j] + "day_" + fencang_features[i];
				featureList.add(featureName);
			}
		}
		for (int i = 0; i < fencang_single_features.length; i++)
			featureList.add(fencang_single_features[i]);

		// if (record.getBigint("flag") == 1L) {
		// for (int i = 0; i < days_fencang.length; i++) {
		// featureList.add("_" + days_fencang[i] + "day_unum_alipay_njhs_avg");
		// }
		// }

		changeSingleModelFeature(featureList, key, false);
		return featureList;
	}

	private void changeSingleModelFeature(ArrayList<String> featureList, Record key, boolean all) {
		// // TODO Auto-generated method stub
		// long val = key.getBigint("val") % 28;
		//
		// int[] models = { 1, 13, 15, 25, 26 };
		// updateFeatures(models, val, featureList,
		// "_365day_divide_366day_qty_alipay_njhs");
		//
		// models = new int[] { 0, 3 };
		// String[] array = all_features;
		// if (!all)
		// array = fencang_features;
		// int[] dayArray = days_all;
		// if (!all)
		// dayArray = days_fencang;
		//
		// for (int i = 0; i < array.length; i++)
		// updateFeatures(models, val, featureList, "_" + 21 + "day_" +
		// array[i]);
		//
		// models = new int[] { 0, 15, 21, 27, };
		// for (int i = 0; i < dayArray.length; i++) {
		// updateFeatures(models, val, featureList, "_" + dayArray[i] + "day_" +
		// "pv_ipv_avg");
		// updateFeatures(models, val, featureList, "_" + dayArray[i] + "day_" +
		// "cart_ipv_avg");
		// }
		//
		// models = new int[] { 1, 2, 4, 9, 14, 16, 17, 18, 19, 23, 24, 25, 5,
		// 10, 11, 12, 13, };
		// for (int i = 0; i < dayArray.length; i++) {
		// updateFeatures(models, val, featureList, "_" + dayArray[i] + "day_" +
		// "pv_uv_avg");
		// updateFeatures(models, val, featureList, "_" + dayArray[i] + "day_" +
		// "cart_uv_avg");
		// }
		//
		// models = new int[] { 2, 6, 13, };
		// for (int i = 0; i < dayArray.length; i++) {
		// updateFeatures(models, val, featureList, "_" + dayArray[i] + "day_" +
		// "brow_buy_rate");
		// updateFeatures(models, val, featureList, "_" + dayArray[i] + "day_" +
		// "cart_buy_rate");
		// updateFeatures(models, val, featureList, "_" + dayArray[i] + "day_" +
		// "collect_buy_rate");
		// }
		//
		// models = new int[] { 1, 3, 13, 25 };
		// for (int i = 2; i <= 7; i++)
		// updateFeatures(models, val, featureList, "_single_" + i +
		// "day_qty_alipay_njhs");
	}

	private void updateFeatures(int[] models, long val, ArrayList<String> featureList, String featureName) {
		// TODO Auto-generated method stub
		boolean has = false;
		for (int i = 0; i < models.length; i++)
			if (val == models[i])
				has = true;
		if (!has)
			return;
		featureList.add(featureName);
	}

	private ArrayList<String> getAllFeatures(Record key, Record record) {
		ArrayList<String> featureList = new ArrayList<String>();
		for (int j = 0; j < days_all.length; j++) {
			for (int i = 0; i < all_features.length; i++) {
				String featureName = "_" + days_all[j] + "day_" + all_features[i];
				featureList.add(featureName);
			}
		}
		// if (record.getBigint("flag") == 1L) {
		// for (int i = 0; i < days_all.length; i++) {
		// featureList.add("_" + days_all[i] + "day_unum_alipay_njhs_avg");
		// }
		// }
		for (int i = 0; i < all_single_features.length; i++)
			featureList.add(all_single_features[i]);
		changeSingleModelFeature(featureList, key, true);
		return featureList;
	}

	private long val;

	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		ArrayList<DataSet> dataList = new ArrayList<DataSet>();
		ArrayList<DataSet> testList = new ArrayList<DataSet>();
		val = key.getBigint("val");
		double learning_rate = 0.1, alpha = 0.5, feature_rate = 1, sample_rate = 1;
		int n_estimator = 200;
		int max_depth = 3;
		int flag = 0;
		TreeInfo[][] infoList = new TreeInfo[505][205];
		while (values.hasNext()) {
			Record record = values.next();
			if (record.getBigint("is_tree") == 1L) {
				TreeInfo info = new TreeInfo(record.getBigint("root_id"), record.getBigint("left_son"),
						record.getBigint("right_son"), record.getBigint("split_feature"),
						record.getDouble("split_feature_value"), record.getBigint("estimator_num"),
						record.getBigint("is_root"), record.getDouble("node_value"));
				infoList[(int) info.estimator_num][(int) info.root_id] = info;
			} else {
				ArrayList<String> features = null;
				learning_rate = record.getDouble("learning_rate");
				alpha = record.getDouble("alpha");
				sample_rate = record.getDouble("sample_rate");
				feature_rate = record.getDouble("feature_rate");
				max_depth = record.getBigint("max_depth").intValue();
				n_estimator = record.getBigint("n_estimator").intValue();
				if (record.getString("store_code").equals("all"))
					features = getAllFeatures(key, record);
				else
					features = getFencangFeatures(key, record);
				int n_feature = features.size();

				DataSet dataSet = new DataSet();
				dataSet.X = new Sample(n_feature);
				dataSet.item_id = record.getBigint("item_id");
				dataSet.store_code = record.getString("store_code");
				int cnt = 0;

				for (String feature : features)
					dataSet.X.a[cnt++] = Double.valueOf(record.get(feature).toString());

				// dataSet.X.a[cnt++] = record.getDouble("targeta");
				// dataSet.X.a[cnt++] = record.getDouble("targetb");
				dataSet.Y = record.getBigint("y");
				dataSet.sample_weight = record.getDouble("sample_weight");
				dataSet.targeta = record.getDouble("targeta");
				dataSet.targetb = record.getDouble("targetb");
				Long train = record.getBigint("train");
				if (train == 0)
					testList.add(dataSet);
				else
					dataList.add(dataSet);
				flag = record.getBigint("flag").intValue();
			}
		}
		Sample[] trainSamples = new Sample[dataList.size()];
		double[] Y = new double[dataList.size()];
		double[] sample_weight_array = new double[dataList.size()];
		for (int i = 0; i < dataList.size(); i++) {
			DataSet dataSet = dataList.get(i);
			trainSamples[i] = dataSet.X;
			Y[i] = dataSet.Y;
			sample_weight_array[i] = dataSet.sample_weight;
		}

		// n_estimator = 100;
		// n_estimator = Math.max(150, n_estimator);

		
		dataList.clear();
		dataList = null;
		System.gc();

		GradientBoostingRegressor gbrt = new GradientBoostingRegressor(learning_rate, n_estimator, max_depth, alpha,
				sample_rate, feature_rate);
		System.out.println("fitting");
		gbrt.fit(trainSamples, Y, sample_weight_array, context, infoList);
		System.out.println("ok!");

		ArrayList<TreeInfo> resultInfoList = gbrt.getTreeInfo();
		for (TreeInfo info : resultInfoList) {
			result.setBigint("item_id", -2L);
			result.setBigint("root_id", info.root_id);
			result.setBigint("left_son", info.left_son);
			result.setBigint("right_son", info.right_son);
			result.setBigint("split_feature", info.split_feature);
			result.setDouble("split_feature_value", info.split_feature_value);
			result.setBigint("estimator_num", info.estimator_num);
			result.setDouble("node_value", info.node_value);
			result.setBigint("is_root", info.is_root);
			result.setBigint("val", key.getBigint("val"));
			context.write(result);
		}

		double total_cost = 0;
		if (gbrt.n_estimator >= 100) {
			for (int i = 0; i < testList.size(); i++) {
				DataSet dataSet = testList.get(i);
				result.setBigint("item_id", dataSet.item_id);
				result.setString("store_code", dataSet.store_code);
				result.setBigint("y", (long) dataSet.Y);
				result.setDouble("targeta", dataSet.targeta);
				result.setDouble("targetb", dataSet.targetb);
				double predictY = gbrt.predict(new Sample[] { dataSet.X })[0];
				result.setDouble("predicty", predictY);
				context.write(result);
				if (predictY < dataSet.Y)
					total_cost += (dataSet.Y - predictY) * dataSet.targeta;
				else
					total_cost -= (dataSet.Y - predictY) * dataSet.targetb;
			}
		}
//		if (gbrt.n_estimator == 180) {
//			for (int i = 100; i <= 180; i += 10) {
//				total_cost = 0;
//				for (int j = 0; j < testList.size(); j++) {
//					DataSet dataSet = testList.get(j);
//					double predictY = gbrt.predict(new Sample[] { dataSet.X }, i)[0];
//					if (predictY < dataSet.Y)
//						total_cost += (dataSet.Y - predictY) * dataSet.targeta;
//					else
//						total_cost -= (dataSet.Y - predictY) * dataSet.targetb;
//				}
//				result.setDouble("node_value", alpha);
//				result.setString("store_code", "" + key.getBigint("val") % 10 + " " + i);
//				result.setDouble("predicty", total_cost);
//				result.setBigint("item_id", -3L);
//				result.setBigint("y", i + 0L);
//				context.write(result);
//			}
//		}
		result.setDouble("node_value", alpha);
		result.setString("store_code", "" + key.getBigint("val") % 10 + " " + flag);
		result.setDouble("predicty", total_cost);
		result.setBigint("item_id", -1L);
		result.setBigint("y", gbrt.n_estimator + 0L);
		context.write(result);
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
