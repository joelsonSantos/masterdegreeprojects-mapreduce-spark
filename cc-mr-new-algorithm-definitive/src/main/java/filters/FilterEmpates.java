package filters;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import extra.methods.MinimumSpanningTree;

public class FilterEmpates implements
		Function<Tuple2<Integer, MinimumSpanningTree>, Boolean> {

	private static final long serialVersionUID = 1L;
	private Tuple2<Integer, MinimumSpanningTree> first;

	public FilterEmpates(Tuple2<Integer, MinimumSpanningTree> first) {
		this.first = first;
	}

	public Boolean call(Tuple2<Integer, MinimumSpanningTree> v1)
			throws Exception {
		if ((v1._2().getVertice1() == first._2().getVertice1())
				&& (v1._2().getVertice2() == first._2().getVertice2())
				&& (v1._2().getWeight() == first._2().getWeight())) {
            return false;
		}
		return true;
	}

}
