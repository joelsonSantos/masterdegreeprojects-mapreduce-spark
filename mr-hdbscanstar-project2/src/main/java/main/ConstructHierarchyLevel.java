package main;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class ConstructHierarchyLevel implements
		PairFunction<Tuple2<Integer, int[]>, Integer, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>> {

	private static final long serialVersionUID = 1L;
	private int oldLabels;

	public ConstructHierarchyLevel(int labels) {
		this.oldLabels = labels;
	}

	@Override
	public Tuple2<Integer, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>> call(
			Tuple2<Integer, int[]> elements) throws Exception {
		
		// old cluster label, (information of new cluster candidate)
		return new Tuple2<Integer, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>>(
				this.oldLabels, new Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>(
						new Tuple3<Integer, int[], Boolean>(this.oldLabels, elements._2(), false), null));
	}
}
