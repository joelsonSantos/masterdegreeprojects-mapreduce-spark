package main;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import scala.Tuple3;

public class LabelingClusters implements
		Function2<Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int minCSize;
	private int label;

	public LabelingClusters(int label, int minClusterSize) {
		this.minCSize = minClusterSize;
		this.label = label;
	}

	@Override
	public Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>> call(
			Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>> arg0,
			Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>> arg1) throws Exception {
		boolean newCluster = false;
		if ((arg0._1()._2().length > this.minCSize) && (arg1._1()._2().length > this.minCSize)) {
			newCluster = true;
		}
		return new Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>(
				new Tuple3<Integer, int[], Boolean>(arg0._1()._1(), arg0._1()._2(), newCluster),
				new Tuple3<Integer, int[], Boolean>(arg1._1()._1(), arg1._1()._2(), newCluster));
	}

}
