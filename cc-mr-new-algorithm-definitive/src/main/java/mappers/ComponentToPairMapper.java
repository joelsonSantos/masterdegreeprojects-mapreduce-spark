package mappers;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ComponentToPairMapper
		implements PairFunction<String, Integer, Integer> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, Integer> call(String t) throws Exception {
		String[] s = t.split(" ");
		return new Tuple2(Integer.parseInt(s[0]), Integer.parseInt(s[1]));
	}
}
