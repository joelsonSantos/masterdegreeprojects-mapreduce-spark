package mappers;

import java.util.LinkedList;
import java.util.TreeSet;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RefreshLabelsMapper implements PairFunction<String, Integer, TreeSet<String>> {

	private static final long serialVersionUID = 1L;
	private LinkedList<Tuple2<Integer, Integer>> listSizeCC;
    private double currentEdge;
	
	public RefreshLabelsMapper(LinkedList<Tuple2<Integer, Integer>> list, double current) {
		this.listSizeCC = list;
		this.currentEdge = current;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, TreeSet<String>> call(String v1) throws Exception {

		String[] s = v1.split(" ");
		Integer v = Integer.parseInt(s[0]);
		Integer u = Integer.parseInt(s[1]);

		StringBuilder string = new StringBuilder();
        Tuple2<Integer, TreeSet<String>> tuple = null;
		
		for (int i = 0; i < this.listSizeCC.size(); i++) {
			////  
			if (v == this.listSizeCC.get(i)._1().intValue()) {
				string.append(v + " " + this.currentEdge + " " + this.listSizeCC.get(i)._2().intValue());
				TreeSet<String> set = new TreeSet<String>();
				set.add(string.toString());
				tuple = new Tuple2(u, set);
			}
		}
		return tuple;
	}
}
