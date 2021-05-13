package main;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple5;

public class OlderLabel implements Function<Tuple2<Integer, Tuple5<int[], int[], double[], Integer, Integer>>, Boolean> {

	private static final long serialVersionUID = 1L;
	private Integer element;

	public OlderLabel(Integer element) {
		this.element = element;
	}
	
	@Override
	public Boolean call(Tuple2<Integer, Tuple5<int[], int[], double[], Integer, Integer>> arg0) throws Exception {
		//return arg0._2()._1() == this.element.intValue();
		return false;
	}
}
