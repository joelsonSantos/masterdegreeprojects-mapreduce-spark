package mappers;

import org.apache.spark.api.java.function.PairFunction;

import extra.methods.Clusters;
import scala.Tuple2;

public class FlatPartitionMapper implements
		PairFunction<Tuple2<Integer, Clusters>, Integer, Clusters> {

	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, Clusters> call(
			Tuple2<Integer, Clusters> t) throws Exception {
		return new Tuple2(t._1(), t._2());
	}
}
