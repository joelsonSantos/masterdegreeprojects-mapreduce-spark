package mappers;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ConstructADJMapper implements PairFunction<String, Integer, String> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, String> call(String t) throws Exception {
		String[] edges = t.split(" ");
		//TreeSet<Integer> set = new TreeSet<Integer>();
		//set.add(Integer.parseInt(edges[1]));
		//StringBuilder set = new StringBuilder();
		//set.append(edges[1]);
		return new Tuple2(Integer.parseInt(edges[0]), edges[1]);
	}
}
