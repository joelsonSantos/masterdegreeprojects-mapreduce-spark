package filters;

import org.apache.spark.api.java.function.Function;

import extra.methods.Clusters;
import scala.Tuple2;

public class SolutionFilter
		implements Function<Tuple2<Integer, Clusters>, Boolean> {

	private static final long serialVersionUID = 1L;
	private int solution;

	public SolutionFilter(int solution) {
		this.solution = solution;
	}

	public Boolean call(Tuple2<Integer, Clusters> v1) throws Exception {

		if (v1._2().getId() == solution) {
			return true;
		}
		return false;
	}

}
