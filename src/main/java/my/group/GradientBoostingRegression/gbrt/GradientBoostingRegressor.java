package my.group.GradientBoostingRegression.gbrt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;

import com.aliyun.odps.mapred.Reducer.TaskContext;

import my.group.GradientBoostingRegression.gbrt.DecisionRegressionTree.Node;
import my.group.GradientBoostingRegression.gbrt.util.Estimator;
import my.group.GradientBoostingRegression.gbrt.util.LossFunction;
import my.group.GradientBoostingRegression.gbrt.util.QuantileEstimator;
import my.group.GradientBoostingRegression.gbrt.util.QuantileLossFunction;
import my.group.GradientBoostingRegression.gbrt.util.Utils.Item;
import my.group.GradientBoostingRegression.gbrt.DecisionRegressionTree.TrainSample;
import my.group.GradientBoostingRegression.gbrt.DecisionRegressionTree.TrainSample.Flag;;

public class GradientBoostingRegressor {

	private double learning_rate;
	public int n_estimator;
	private double alpha;
	private double subsample = 1.0;
	private Estimator init_;
	private LossFunction loss;
	private int random_state;
	private DecisionRegressionTree[] trees;
	private int max_depth;
	private double baseValue;
	private double[] residual;

	public GradientBoostingRegressor(double learning_rate, int n_estimator, int max_depth, double alpha,
			double sample_rate, double feature_rate) {
		this.learning_rate = learning_rate;
		this.n_estimator = n_estimator;
		this.max_depth = max_depth;
		this.sample_rate = sample_rate;
		this.feature_rate = feature_rate;
		this.trees = new DecisionRegressionTree[this.n_estimator];
		this.alpha = alpha;
		this.loss = new QuantileLossFunction(alpha);
		this.init_ = new QuantileEstimator(alpha);
		this.random_state = new Random().nextInt();
		for (int i = 0; i < MAX_SAMPLE; i++) {
			samples[i] = new TrainSample(null, 0);
			items[i] = new Item(0, 0);
		}
	}

	public static class TreeInfo {
		public long root_id;
		public long left_son;
		public long right_son;
		public long split_feature;
		public double split_feature_value;
		public long estimator_num;
		public long is_root;
		public double node_value;

		public TreeInfo(long root_id, long left_son, long right_son, long split_feature, double split_feature_value,
				long estimator_num, long is_root, double node_value) {
			this.root_id = root_id;
			this.left_son = left_son;
			this.right_son = right_son;
			this.split_feature = split_feature;
			this.split_feature_value = split_feature_value;
			this.estimator_num = estimator_num;
			this.is_root = is_root;
			this.node_value = node_value;
		}

	}

	private int MAX_SAMPLE = 155005;
	private int MAX_FEATURE = 205;
	private TrainSample[] samples = new TrainSample[MAX_SAMPLE];
	private TrainSample[][] sortedSampleArrays = new TrainSample[MAX_FEATURE][MAX_SAMPLE];
	private TrainSample[][] sampleArrays = new TrainSample[MAX_FEATURE][MAX_SAMPLE];
	private double[] splitValueArray = new double[MAX_SAMPLE];
	private int[] randomArray = new int[MAX_SAMPLE];
	private Item[] items = new Item[MAX_SAMPLE];
	private double[] weight_sum = new double[MAX_SAMPLE];
	private double sample_rate;
	private double feature_rate;

	private double[] _fit_stage(int i, Sample[] X, double[] Y, double[] y_pred, double[] sample_weight, long start,
			long last, TreeInfo[] infoList) {
		this.loss.negative_gradient(Y, y_pred, this.residual);
		if (infoList[0] == null) {
			int n_features = X[0].a.length;
			int n_samples = X.length;
			for (int a = 0; a < n_features; a++) {
				for (int b = 0; b < n_samples; b++) {
					sortedSampleArrays[a][b] = sampleArrays[a][b];
					sortedSampleArrays[a][b].init(X, residual, sample_weight, flag);
				}
			}
			trees[i] = new DecisionRegressionTree(i, this.max_depth, this.random_state, samples, splitValueArray,
					sortedSampleArrays, randomArray, sample_rate, feature_rate, context);
			trees[i].fit(X, residual, sample_weight, start, last);
		} else {
			trees[i] = new DecisionRegressionTree(i, infoList);
		}
		if (trees[i].exit)
			return y_pred;
		this.loss.update_terminal_region(trees[i], X, Y, y_pred, sample_weight);
		for (int j = 0; j < X.length; j++) {
			Node leaf = trees[i].apply(X[j]);
			y_pred[j] += learning_rate * leaf.treeVal;
		}
		return y_pred;
	}

	private Flag[] flag;
	private TaskContext context;

	private int _fit_stages(Sample[] X, double[] Y, double[] y_pred, double[] sample_weight, TreeInfo[][] infoList) {

		int n_features = X[0].a.length;
		int n_samples = X.length;
		flag = new TrainSample.Flag[n_samples];
		for (int i = 0; i < n_samples; i++)
			flag[i] = new Flag();
		for (int a = 0; a < n_features; a++) {
			for (int b = 0; b < n_samples; b++)
				sampleArrays[a][b] = new TrainSample(X[b], b);

			final int compareFeature = a;
			Arrays.sort(sampleArrays[a], 0, n_samples, new Comparator<TrainSample>() {

				@Override
				public int compare(TrainSample o1, TrainSample o2) {
					return new Double(o1.X.a[compareFeature]).compareTo(o2.X.a[compareFeature]);
				}
			});
		}
		this.residual = new double[X.length];
		long start = System.currentTimeMillis();
		long last = start;
		for (int i = 0; i < this.n_estimator; i++) {
			System.out.println("第" + i + "棵树");
			y_pred = _fit_stage(i, X, Y, y_pred, sample_weight, start, last, infoList[i]);
			if (System.currentTimeMillis() - last > 1000 * 60 * 4) {
				context.progress();
				last = System.currentTimeMillis();
			}
			if (trees[i] == null || trees[i].exit) {
				this.n_estimator = i;
				break;
			}
			last = trees[i].last;
		}
		return this.n_estimator;
	}

	public void fit(Sample[] X, double[] Y, double[] sample_weight, TaskContext context, TreeInfo[][] infoList) {
		this.init_.fit(X, Y, sample_weight, items, weight_sum);
		double[] y_pred = this.init_.predict(X);
		this.baseValue = y_pred[0];
		this.context = context;
		_fit_stages(X, Y, y_pred, sample_weight, infoList);
	}

	public double[] predict(Sample[] X) {
		double[] ans = new double[X.length];
		for (int i = 0; i < X.length; i++) {
			ans[i] = baseValue;
			for (int j = 0; j < n_estimator; j++) {
				DecisionRegressionTree.Node leaf = trees[j].apply(X[i]);
				ans[i] += learning_rate * leaf.treeVal;
			}
		}
		return ans;
	}
	
	public double[] predict(Sample[] X,int n) {
		double[] ans = new double[X.length];
		for (int i = 0; i < X.length; i++) {
			ans[i] = baseValue;
			for (int j = 0; j < n; j++) {
				DecisionRegressionTree.Node leaf = trees[j].apply(X[i]);
				ans[i] += learning_rate * leaf.treeVal;
			}
		}
		return ans;
	}

	public ArrayList<TreeInfo> getTreeInfo() {
		ArrayList<TreeInfo> infoList = new ArrayList<TreeInfo>();
		for (int i = 0; i < this.n_estimator; i++) {
			infoList.addAll(trees[i].getTreeInfo());
		}
		return infoList;
	}

}