package main;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
import scala.Tuple5;
import scala.Tuple7;

public class LastLabels
		implements FlatMapFunction<Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>>, Integer> {

	private static final long serialVersionUID = 1L;
	private int object;

	public LastLabels(int object) {
		this.object = object;
	}
	
	@Override
	public Iterator<Integer> call(Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>> arg0)
			throws Exception {
		// return only the last label.
		return Arrays.asList(arg0._2()._1()[object]).iterator();
	}

}
