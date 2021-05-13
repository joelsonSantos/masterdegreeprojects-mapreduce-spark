package mappers;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Refresh implements
		PairFunction<Tuple2<Integer, Tuple2<Integer, String>>, Integer, String> {

	private static final long serialVersionUID = 1L;
	private double currentEdge;
	
    public Refresh(double currentEdge){
    	this.currentEdge = currentEdge;
    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple2<Integer, String> call(Tuple2<Integer, Tuple2<Integer, String>> t) throws Exception {
		int v = t._1();
		int u = t._2()._1();
		String[] str = t._2()._2().split(" ");

		StringBuilder string = new StringBuilder();
        Tuple2<Integer, String> tuple = null;
				string.append(v + " " + this.currentEdge + " " + str[0] + " " + str[1]);
				tuple = new Tuple2(u, string.toString());
		return tuple;
	}

}
