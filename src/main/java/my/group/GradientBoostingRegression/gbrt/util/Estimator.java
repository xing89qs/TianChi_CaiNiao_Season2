package my.group.GradientBoostingRegression.gbrt.util;

import my.group.GradientBoostingRegression.gbrt.Sample;
import my.group.GradientBoostingRegression.gbrt.util.Utils.Item;

public abstract class Estimator {
	public abstract void fit(Sample[] X, double[] y, double[] sample_weight, Item[] items, double[] weight_sum);

	public abstract double[] predict(Sample[] X);
}
