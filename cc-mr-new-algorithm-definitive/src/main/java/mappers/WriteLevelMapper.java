package mappers;

import org.apache.spark.api.java.function.PairFunction;

import extra.methods.Clusters;
import extra.methods.Noise;
import scala.Tuple2;

public class WriteLevelMapper
		implements PairFunction<String, Integer, Clusters> {

	private static final long serialVersionUID = 1L;

	public WriteLevelMapper() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, Clusters> call(String t) throws Exception {
		String[] edges = t.split(" ");
		// key : (object)(0) - value: clusterID(1) , currentLevel(2), sizeCC(3)
		return new Tuple2(Integer.parseInt(edges[0]),
				new Clusters(Integer.parseInt(edges[1]),
						Integer.parseInt(edges[4]), Integer.parseInt(edges[3]),
						Double.parseDouble(edges[2]), 0.0, null, 0, new Noise(),
						null));
	}
}
