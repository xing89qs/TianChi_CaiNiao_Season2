package my.group.GradientBoostingRegression.gbrt;

import java.util.ArrayList;
import java.util.Random;
import java.util.Stack;

import com.aliyun.odps.mapred.Reducer.TaskContext;

import my.group.GradientBoostingRegression.gbrt.GradientBoostingRegressor.TreeInfo;

public class DecisionRegressionTree {

	public static class Node {
		double split_val;
		int split_feature;
		public Node leftNode;
		public Node rightNode;
		public double treeVal;

		public ArrayList<Double> diff = new ArrayList<Double>();
		public ArrayList<Double> diff_sample_weight = new ArrayList<Double>();

		public void setTreeValue(double value) {
			this.treeVal = value;
		}

		public void clear() {
			this.diff.clear();
			this.diff.trimToSize();
			this.diff_sample_weight.clear();
			this.diff_sample_weight.trimToSize();
			this.diff = null;
			this.diff_sample_weight = null;
		}
	}

	private final long PROGRESS_TIME = 1000 * 60 * 4;

	private final long MAX_RUNNING_TIME = 1000 * 60 * 9;

	private int max_depth;
	private int random_state;
	private Random random;
	private Node root;

	private TrainSample[][] sampleArrays;

	boolean exit;

	private long start;

	long last;

	private TaskContext context;

	private double sample_rate;

	private double feature_rate;

	private int estimator_num;

	private void checkTime() {
		if (System.currentTimeMillis() - start > MAX_RUNNING_TIME) {
			exit = true;
			return;
		}
		if (System.currentTimeMillis() - last > PROGRESS_TIME) {
			last = System.currentTimeMillis();
			context.progress();
		}
	}

	public DecisionRegressionTree(int estimator_num, int max_depth, int random_state, TrainSample[] samples,
			double[] splitValueArray, TrainSample[][] sampleArrays, int[] randomArray, double sample_rate,
			double feature_rate, TaskContext context) {
		this.estimator_num = estimator_num;
		this.max_depth = max_depth;
		this.random_state = random_state;
		this.random = new Random(this.random_state);
		this.sampleArrays = sampleArrays;
		this.samples = samples;
		this.splitValueArray = splitValueArray;
		this.exit = false;
		this.context = context;
		this.sample_rate = sample_rate;
		this.feature_rate = feature_rate;
	}

	public DecisionRegressionTree(int estimator_num, TreeInfo[] infoList) {
		this.estimator_num = estimator_num;
		Node[] nodeList = new Node[infoList.length];
		for (int i = 0; i < infoList.length; i++) {
			if (infoList[i] == null)
				continue;
			Node node = new Node();
			nodeList[(int) infoList[i].root_id] = node;
			if ((int) infoList[i].left_son != -1)
				node.leftNode = nodeList[(int) infoList[i].left_son];
			if ((int) infoList[i].right_son != -1)
				node.rightNode = nodeList[(int) infoList[i].right_son];
			node.split_feature = (int) infoList[i].split_feature;
			node.split_val = infoList[i].split_feature_value;
			node.treeVal = infoList[i].node_value;
			if (infoList[i].is_root == 1L)
				this.root = node;
		}
	}

	class SplitResult {
		Stack<TrainSample> leftSample, rightSample;
		double split_error, left_error, right_error;
		int best_feature;
		double best_split_val;

		// error = var_left + var_right
		// var_left = sigma((y_i-y_bar)^2*w)
		// = sigma(y_i*y_i*w_i)- y_bar*sigma(2*w_i*y_i)+ y_bar*y_bar*sigma(w_i)
		// y_bar = sigma(y_i*w_i)/sigma(w_i)

		double left_yyw_sum, left_y_w_sum, left_w_sum, left_bar;
		double right_yyw_sum, right_y_w_sum, right_w_sum, right_bar;

		public SplitResult(int l, int r, TrainSample[] samples) {
			leftSample = new Stack<TrainSample>();
			rightSample = new Stack<TrainSample>();
			init(l, r, samples);
		}

		void init(int l, int r, TrainSample[] samples) {
			this.split_error = Double.MAX_VALUE;
			leftSample.clear();
			rightSample.clear();

			left_yyw_sum = left_y_w_sum = left_w_sum = left_bar = 0;
			right_yyw_sum = right_y_w_sum = right_w_sum = right_bar = 0;
			for (int i = r; i >= l; i--) {
				if (!samples[i].flag.sample)
					continue;
				checkTime();
				if (exit)
					return;
				rightSample.push(samples[i]);
				right_yyw_sum += samples[i].y * samples[i].y * samples[i].sample_weight;
				right_y_w_sum += samples[i].y * samples[i].sample_weight;
				right_w_sum += samples[i].sample_weight;
			}
			right_bar = right_y_w_sum / right_w_sum;
			left_error = left_yyw_sum - 2 * left_y_w_sum * left_bar + left_bar * left_bar * left_w_sum;
			right_error = right_yyw_sum - 2 * right_y_w_sum * right_bar + right_bar * right_bar * right_w_sum;
			split_error = left_error + right_error;
		}

		void moveSampleToLeft(int split_feature, double split_val) {
			while (!rightSample.empty()) {
				TrainSample sample = rightSample.peek();
				if (sample.X.a[split_feature] < split_val) {
					rightSample.pop();
					right_yyw_sum -= sample.y * sample.y * sample.sample_weight;
					right_y_w_sum -= sample.y * sample.sample_weight;
					right_w_sum -= sample.sample_weight;

					leftSample.push(sample);
					left_yyw_sum += sample.y * sample.y * sample.sample_weight;
					left_y_w_sum += sample.y * sample.sample_weight;
					left_w_sum += sample.sample_weight;
				} else
					break;
				checkTime();
				if (exit)
					return;
			}
			left_bar = left_y_w_sum / left_w_sum;
			right_bar = right_y_w_sum / right_w_sum;
			left_error = left_yyw_sum - 2 * left_y_w_sum * left_bar + left_bar * left_bar * left_w_sum;
			right_error = right_yyw_sum - 2 * right_y_w_sum * right_bar + right_bar * right_bar * right_w_sum;
			split_error = left_error + right_error;
		}

		Stack<TrainSample> getLeftTrainSamples() {
			return leftSample;
		}

		Stack<TrainSample> getRightTrainSamples() {
			return rightSample;
		}

		void clear() {
			this.leftSample.clear();
			this.rightSample.clear();
			this.leftSample = this.rightSample = null;
		}

	}

	private SplitResult getBestSplit(int l, int r) {

		int n_features = sampleArrays[0][0].X.a.length;
		double min_split_error = Double.MAX_VALUE;
		int best_feature = -1;
		double best_split_val = Double.MAX_VALUE;

		for (int i = l; i <= r; i++) {
			if (random.nextDouble() > sample_rate)
				sampleArrays[0][i].flag.sample = false;
			else
				sampleArrays[0][i].flag.sample = true;
		}
		SplitResult currentResult = new SplitResult(l, r, sampleArrays[0]);
		for (int i = 0; i < n_features; i++) {
			checkTime();
			if (exit)
				return null;
			if (random.nextDouble() > feature_rate)
				continue;
			int cnt = 0;
			for (int j = l; j < r; j++) {
				splitValueArray[cnt++] = (sampleArrays[i][j].X.a[i] + sampleArrays[i][j + 1].X.a[i]) / 2.0;
			}
			if (cnt == 0)
				continue;
			currentResult.init(l, r, sampleArrays[i]);
			for (int j = 0; j < cnt; j++) {
				double split_val = splitValueArray[j];
				currentResult.moveSampleToLeft(i, split_val);
				if (exit)
					return null;
				if (currentResult.split_error < min_split_error) {
					min_split_error = currentResult.split_error;
					best_feature = i;
					best_split_val = split_val;
				}
			}
		}
		if (best_feature == -1)
			return null;
		for (int i = l; i <= r; i++)
			sampleArrays[best_feature][i].flag.sample = true;
		SplitResult result = new SplitResult(l, r, sampleArrays[best_feature]);
		result.moveSampleToLeft(best_feature, best_split_val);
		result.best_feature = best_feature;
		result.best_split_val = best_split_val;
		return result;

	}

	private Node createTree(int l, int r, int depth) {
		if (depth > max_depth)
			return null;
		if (this.exit)
			return null;
		Node root = new Node();
		if (l >= r)
			return root;
		SplitResult result = getBestSplit(l, r);
		if (this.exit)
			return null;
		if (result == null)
			return root;
		root.split_feature = result.best_feature;
		root.split_val = result.best_split_val;
		Stack<TrainSample> leftSamples = result.getLeftTrainSamples();
		Stack<TrainSample> rightSamples = result.getRightTrainSamples();
		for (TrainSample sample : leftSamples)
			sample.flag.left = true;
		for (TrainSample sample : rightSamples)
			sample.flag.left = false;
		int leftSize = leftSamples.size();
		int n_features = sampleArrays[0][0].X.a.length;
		for (int i = 0; i < n_features; i++) {
			checkTime();
			int tot = l;
			for (int j = l; j <= r; j++) {
				if (sampleArrays[i][j].flag.left) {
					samples[tot++] = sampleArrays[i][j];
				}
			}
			for (int j = l; j <= r; j++) {
				if (!sampleArrays[i][j].flag.left)
					samples[tot++] = sampleArrays[i][j];
			}
			for (int j = l; j <= r; j++)
				sampleArrays[i][j] = samples[j];

		}
		result.clear();
		root.leftNode = createTree(l, l + leftSize - 1, depth + 1);
		root.rightNode = createTree(l + leftSize, r, depth + 1);
		return root;
	}

	static class TrainSample {
		public TrainSample(Sample sample, int index) {
			this.X = sample;
			this.index = index;
		}

		Sample X;
		int index;
		double y, sample_weight;

		static class Flag {
			public boolean sample;
			boolean left;
		}

		Flag flag;

		public void init(Sample[] sample, double[] y, double[] sample_weight, Flag[] flag) {
			// TODO Auto-generated method stub
			this.flag = flag[index];
			this.y = y[index];
			this.sample_weight = sample_weight[index];
			this.X = sample[index];
		}
	}

	private TrainSample[] samples;
	private double splitValueArray[];

	public void fit(Sample[] X, double[] Y, double[] sample_weight, long start, long last) {
		random_state = random.nextInt();
		this.start = start;
		this.last = last;
		Node root = createTree(0, X.length - 1, 0);
		this.root = root;
	}

	private Node dfs(Node root, Sample x, int depth) {
		if (root.leftNode == null)
			return root;
		if (x.a[root.split_feature] < root.split_val)
			return dfs(root.leftNode, x, depth + 1);
		return dfs(root.rightNode, x, depth + 1);
	}

	public Node apply(Sample x) {
		return dfs(root, x, 0);
	}

	private int nodeCount = 0;

	private int dfsNode(Node node, ArrayList<TreeInfo> infoList, int is_root) {
		if (node == null)
			return -1;
		int left_son = dfsNode(node.leftNode, infoList, 0);
		int right_son = dfsNode(node.rightNode, infoList, 0);
		int id = nodeCount++;
		TreeInfo info = new TreeInfo(id, left_son, right_son, node.split_feature, node.split_val, estimator_num,
				is_root, node.treeVal);
		infoList.add(info);
		return id;
	}

	public ArrayList<TreeInfo> getTreeInfo() {
		nodeCount = 0;
		ArrayList<TreeInfo> infoList = new ArrayList<TreeInfo>();
		dfsNode(root, infoList, 1);
		return infoList;
	}
}
