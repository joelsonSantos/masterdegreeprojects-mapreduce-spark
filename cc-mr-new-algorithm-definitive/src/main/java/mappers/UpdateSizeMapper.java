package mappers;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class UpdateSizeMapper implements
		PairFunction<Tuple2<Integer, Integer>, Integer, Integer> {

	private static final long serialVersionUID = 1L;
	

	public UpdateSizeMapper() {
		
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t)
			throws Exception {
		// vSource, sizeCC
		return new Tuple2(t._1().intValue(), t._2().intValue());
	}

	
}
