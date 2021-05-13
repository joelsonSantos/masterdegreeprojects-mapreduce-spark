package mappers;

import org.apache.spark.api.java.function.PairFunction;

import extra.methods.Clusters;
import scala.Tuple2;

public class Change
		implements PairFunction<Tuple2<Integer, Clusters>, Integer, Clusters> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, Clusters> call(Tuple2<Integer, Clusters> t)
			throws Exception {
		Tuple2<Integer, Clusters> tuple = null;
		if (t._2().isNoise()) {
			tuple = new Tuple2(t._2().getId(), t._2());
		} else if(t._2().isNewCluster()){
			tuple = new Tuple2(t._2().getParentCluster().getId(),
					t._2().getParentCluster());
		} else { // propagou o cluster
			tuple = new Tuple2(t._2().getId(), t._2());
		}
		return tuple;
	}
}
