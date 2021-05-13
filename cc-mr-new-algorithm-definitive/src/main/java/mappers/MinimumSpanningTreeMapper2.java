package mappers;

import java.util.LinkedList;

import org.apache.spark.api.java.function.PairFunction;

import extra.methods.MinimumSpanningTree;

import scala.Tuple2;

public class MinimumSpanningTreeMapper2 implements
		PairFunction<String, Double, MinimumSpanningTree> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Double, MinimumSpanningTree> call(String t) throws Exception {
		String[] s = t.split(" ");
	    int v = Integer.parseInt(s[0]);
	    int u = Integer.parseInt(s[1]);
		double w = Double.parseDouble(s[2]);
		LinkedList<MinimumSpanningTree> list = new LinkedList<MinimumSpanningTree>();
		list.add(new MinimumSpanningTree(v, u, w));
	    return new Tuple2(1, new MinimumSpanningTree(list));
	}
}
