package my.group.GradientBoostingRegression.gbrt.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Random;

public class Utils {

	public static class Item implements Comparable<Item> {
		double y;
		double sample_weight;

		public Item(double y, double sample_weight) {
			this.y = y;
			this.sample_weight = sample_weight;
		}

		@Override
		public int compareTo(Item o) {
			return new Double(y).compareTo(o.y);
		}
	}

	public static double weighted_percentile(ArrayList<Double> y, ArrayList<Double> sample_weight, double percentile) {
		Item[] items = new Item[y.size()];
		for (int i = 0; i < y.size(); i++)
			items[i] = new Item(y.get(i), sample_weight.get(i));
		Arrays.sort(items);
		double[] weight_sum = new double[y.size()];
		for (int i = 0; i < y.size(); i++) {
			weight_sum[i] = items[i].sample_weight;
			if (i > 0)
				weight_sum[i] += weight_sum[i - 1];
		}
		double target = weight_sum[y.size() - 1] * percentile / 100.0;
		for (int i = 1; i < y.size(); i++)
			if (weight_sum[i] > target) {
				// System.out.println(y.size() + " " + weight_sum[i] + " " +
				// y.get(i - 1) + " " + target);
				return items[i - 1].y;
			}
		return items[y.size() - 1].y;
	}

	public static double weighted_percentile(double[] y, double[] sample_weight, double percentile) {
		Item[] items = new Item[y.length];
		for (int i = 0; i < y.length; i++)
			items[i] = new Item(y[i], sample_weight[i]);
		Arrays.sort(items);
		double[] weight_sum = new double[y.length];
		for (int i = 0; i < y.length; i++) {
			weight_sum[i] = items[i].sample_weight;
			if (i > 0)
				weight_sum[i] += weight_sum[i - 1];
		}
		double target = weight_sum[y.length - 1] * percentile / 100.0;
		for (int i = 1; i < y.length; i++)
			if (weight_sum[i] > target) {
				// System.out.println(y.length + " " + weight_sum[i] + " " + y[i
				// - 1] + " " + target);
				return items[i - 1].y;
			}
		return items[y.length - 1].y;
	}

	public static void sample(int n, int m, int[] ret) {
		LinkedList<Integer> list = new LinkedList<Integer>();
		for (int i = 0; i < n; i++)
			list.add(i);
		Collections.shuffle(list);
		for (int i = 0; i < m; i++)
			ret[i] = list.pop();
	}
}
