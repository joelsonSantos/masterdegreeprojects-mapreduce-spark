package mappers;

import org.apache.spark.api.java.function.PairFunction;

import extra.methods.Clusters;
import scala.Tuple2;

public class ChangeKeyMapper implements
		PairFunction<Tuple2<Integer, Clusters>, Integer, Clusters> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, Clusters> call(Tuple2<Integer, Clusters> t)
			throws Exception {
		Tuple2<Integer, Clusters> tuple = null;
		if (t._2().isNoise()) {
			if (t._2().isChanged() == 1) {
				tuple = new Tuple2(t._2().getParentCluster().getId(), t._2());
			} else {
				tuple = new Tuple2(t._2().getId(), t._2());
			}
		} else {
			// mudança de id de cluster para id de points
			tuple = new Tuple2(t._2().getId(), new Clusters(t._2().getId(), t._2().getOffsetLevel(), t
					._2().getNumPoints(), t._2().getBirthLevel(), t._2()
					.getStability(), t._2().getParentCluster(), t._2()
					.isChanged(), t._2().getNoise(), t._2().getChildren()));
		}

		return tuple;
	}

}
