package my.group.GradientBoostingRegression.gbrt.util;

import java.util.ArrayList;
import java.util.HashSet;

import my.group.GradientBoostingRegression.gbrt.DecisionRegressionTree;
import my.group.GradientBoostingRegression.gbrt.DecisionRegressionTree.Node;
import my.group.GradientBoostingRegression.gbrt.Sample;
import my.group.GradientBoostingRegression.gbrt.util.Utils.Item;

public class QuantileLossFunction extends LossFunction {

	private double alpha;
	private double percentile;

	public QuantileLossFunction(double alpha) {
		this.alpha = alpha;
		this.percentile = alpha * 100.0;
	}

	public double[] negative_gradient(double[] y_true, double[] y_pred, double[] residual) {
		int size = y_true.length;
		for (int i = 0; i < size; i++) {
			if (y_true[i] > y_pred[i])
				residual[i] = alpha;
			else
				residual[i] = -1.0 + alpha;
		}
		return residual;
	}

	@Override
	public void update_terminal_region(DecisionRegressionTree tree, Sample[] x, double[] y, double[] y_pred,
			double[] sample_weight) {
		int n_samples = x.length;
		HashSet<DecisionRegressionTree.Node> nodeSet = new HashSet<DecisionRegressionTree.Node>();
		for (int i = 0; i < n_samples; i++) {
			DecisionRegressionTree.Node leaf = tree.apply(x[i]);
			leaf.diff.add(y[i] - y_pred[i]);
			leaf.diff_sample_weight.add(sample_weight[i]);
			nodeSet.add(leaf);
		}
		for (Node leaf : nodeSet) {
			leaf.setTreeValue(Utils.weighted_percentile(leaf.diff, leaf.diff_sample_weight, this.percentile));
			// System.out.println(leaf+" "+leaf.treeVal);
			leaf.clear();
		}
	}
}
