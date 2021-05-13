package main;

import java.util.Map;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class ChangeLabel implements PairFunction<Tuple2<Integer, int[]>, Integer, int[]> {

	private static final long serialVersionUID = 1L;
	private Broadcast<Map<Integer, int[]>> bd;

	public ChangeLabel(Broadcast<Map<Integer, int[]>> bd) {
		this.bd = bd;
	}

	@Override
	public Tuple2<Integer, int[]> call(Tuple2<Integer, int[]> arg0) throws Exception {
		int label = arg0._1().intValue();
		for(Integer key : this.bd.getValue().keySet()) {
			for(int i = 0; i < this.bd.getValue().get(key).length; i++) {
			   arg0._2()[this.bd.getValue().get(key)[i]] = key;
			}
		}
		return new Tuple2<Integer, int[]>(label, arg0._2());
	}
}
