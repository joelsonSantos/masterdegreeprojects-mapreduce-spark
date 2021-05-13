package main;

import java.util.Map;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.Tuple6;
import scala.Tuple7;

public class ConstructNewHierarchicalLevel implements
		PairFunction<Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>>, Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>> {

	private static final long serialVersionUID = 1L;
	private Map<Integer, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>> clusters;
	private int label;
	private double weight;
	private int minClusterSize;

	public ConstructNewHierarchicalLevel(
			Map<Integer, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>> clusters, int label,
			double weight, int minClusterSize) {
		this.clusters = clusters;
		this.label = label;
		this.weight = weight;
		this.minClusterSize = minClusterSize;
	}

	@Override
	public Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>> call(
			Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>> arg0) throws Exception {
		// key -> labels, clusters parent labels, cluster children labels, stability of
		// clusters on tree, birth level, next value
		// label for new
		// clusters, last element position on tree (array)
		int nextClusterLabel = arg0._2()._6();
		int childPosition = arg0._2()._7();
		for (Integer key : this.clusters.keySet()) {
			if (this.label == 0) {
				continue;
			}
			int parentPosition = this.label - 1;
			if (this.clusters.get(key)._1()._3()) { // If cluster splits
				for (int i = 0; i < this.clusters.get(key)._1()._2().length; i++) {
					arg0._2()._1()[this.clusters.get(key)._1()._2()[i]] = nextClusterLabel;
				}
				arg0._2()._3()[childPosition] = nextClusterLabel; // children array
				arg0._2()._2()[childPosition] = this.label; // parent array
				if (this.label != 1) {
					arg0._2()._4()[parentPosition] += this.clusters.get(key)._1()._2().length
							* (1 / this.weight - 1 / arg0._2()._5()[parentPosition]);
				}
				childPosition++;
				nextClusterLabel++;
				for (int i = 0; i < this.clusters.get(key)._2()._2().length; i++) {
					arg0._2()._1()[this.clusters.get(key)._2()._2()[i]] = nextClusterLabel;
				}
				arg0._2()._3()[childPosition] = nextClusterLabel; // children array
				arg0._2()._2()[childPosition] = nextClusterLabel;
				if (this.label != 1) {
					arg0._2()._4()[parentPosition] += this.clusters.get(key)._2()._2().length
							* (1 / this.weight - 1 / arg0._2()._5()[parentPosition]);
				}
				childPosition++;
				nextClusterLabel++;
			} else {
				if (this.clusters.get(key)._1()._2().length < this.minClusterSize) {
					for (int i = 0; i < this.clusters.get(key)._1()._2().length; i++) {
						arg0._2()._1()[this.clusters.get(key)._1()._2()[i]] = 0; // noise label
					}
					arg0._2()._4()[parentPosition] += this.clusters.get(key)._1()._2().length
							* (1 / this.weight - 1 / arg0._2()._5()[parentPosition]);
				} else if (this.clusters.get(key)._2()._2().length < this.minClusterSize) {
					for (int i = 0; i < this.clusters.get(key)._2()._2().length; i++) {
						arg0._2()._1()[this.clusters.get(key)._2()._2()[i]] = 0; // noise label
					}
					arg0._2()._4()[parentPosition] += this.clusters.get(key)._2()._2().length
							* (1 / this.weight - 1 / arg0._2()._5()[parentPosition]);
				}
			}
		}
		return new Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>>(1,
				new Tuple7<int[], int[], int[], double[], double[], Integer, Integer>(arg0._2()._1(), arg0._2()._2(),
						arg0._2()._3(), arg0._2()._4(), arg0._2()._5(), nextClusterLabel, childPosition));
	}

}
