package main;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class FilterNoTrueComponents implements Function<Tuple2<Integer, int[]>, Boolean> {

	private static final long serialVersionUID = 1L;
	private int vertex1;
	private int vertex2;

	public FilterNoTrueComponents(int v, int u) {
		this.vertex1 = v;
		this.vertex2 = u;
	}

	@Override
	public Boolean call(Tuple2<Integer, int[]> arg0) throws Exception {
		int element1 = binarySearch(arg0._2(), this.vertex1);
		int element2 = binarySearch(arg0._2(), this.vertex2);
		return (element1 != -1 || element2 != -1);
	}

	public int binarySearch(int[] array, int element) {
		int middle, left, right;
		left = 0;
		right = array.length - 1;
		do {
			middle = (left + right) / 2;
			if (element > array[middle]) {
				left = middle + 1;
			} else {
				right = middle - 1;
			}
		} while (element != array[middle] && left <= right);
		return (element == array[middle]) ? array[middle] : -1;
	}
}
