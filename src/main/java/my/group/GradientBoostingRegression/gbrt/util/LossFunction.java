package my.group.GradientBoostingRegression.gbrt.util;

import my.group.GradientBoostingRegression.gbrt.DecisionRegressionTree;
import my.group.GradientBoostingRegression.gbrt.Sample;

public abstract class LossFunction {
	public abstract double[] negative_gradient(double[] y_true, double[] y_pred, double[] residual);

	public abstract void update_terminal_region(DecisionRegressionTree tree, Sample[] x, double[] y, double[] y_pred,
			double[] sample_weight);
}
