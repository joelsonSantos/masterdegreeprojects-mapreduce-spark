package mappers;

//import java.util.TreeSet;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class EdgesMapper implements
		PairFunction<String, Integer, String> {

	private static final long serialVersionUID = 1L;

	public EdgesMapper() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, String> call(String t) throws Exception {

		String[] edges = t.split(" ");
		Tuple2<Integer, String> tuple = null;

		if (edges.length > 1) {
			//TreeSet<Integer> set = new TreeSet<Integer>();
			//set.add(Integer.parseInt(edges[1]));
			tuple = new Tuple2(Integer.parseInt(edges[0]), edges[1]);
		}
		return tuple;
	}
}
