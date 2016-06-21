package my.group.GradientBoostingRegression.gbrt.util;

import my.group.GradientBoostingRegression.gbrt.Sample;
import my.group.GradientBoostingRegression.gbrt.util.Utils.Item;

public class QuantileEstimator extends Estimator {
	private double alpha;
	private double quantile;

	public QuantileEstimator(double alpha) {
		this.alpha = alpha;
	}

	public void fit(Sample[] X, double[] y, double[] sample_weight, Item[] items, double[] weight_sum) {
		this.quantile = Utils.weighted_percentile(y, sample_weight, this.alpha * 100.0);
	}

	public double[] predict(Sample[] X) {
		int size = X.length;
		double[] ans = new double[size];
		for (int i = 0; i < size; i++)
			ans[i] = this.quantile;
		return ans;
	}
}
