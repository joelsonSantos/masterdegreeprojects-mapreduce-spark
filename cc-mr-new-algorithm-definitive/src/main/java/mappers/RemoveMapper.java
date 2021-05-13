package mappers;

import java.util.LinkedList;
import java.util.TreeSet;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RemoveMapper
		implements
		PairFunction<Tuple2<Integer, String>, Integer, String> {

	private static final long serialVersionUID = 1L;
	private int[][] affectedVertices;
	//private LinkedList<Tuple2<Integer, MinimumSpanningTree>> affectedVertices;

	public RemoveMapper(int[][] affectedVertices) {
		this.setAffectedVertices(affectedVertices);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple2<Integer, String> call(
			Tuple2<Integer, String> t) throws Exception {
		int key = t._1().intValue();
		LinkedList<Integer> remove = new LinkedList<Integer>();

		for (int v = 0; v < this.affectedVertices.length; v++) {
            int keyCurrentEdge = this.affectedVertices[v][0];
            int valueCurrentEdge = this.affectedVertices[v][1];
            int keyReverseCurrentEdge = this.affectedVertices[v][1];
            int valueReverseCurrentEdge = this.affectedVertices[v][0];
			
			if (((key == keyCurrentEdge) || (key == keyReverseCurrentEdge))
					&& (keyCurrentEdge != keyReverseCurrentEdge)) {
				String[] iterator = t._2().split(" ");
				for (int i = 0; i < iterator.length; i++) {
					int value = Integer.parseInt(iterator[i]);
					// String[] s = string.split(" ");
					// int value = Integer.parseInt(s[0]);
					if (((key == keyCurrentEdge) && (value == valueCurrentEdge))
							|| ((key == keyReverseCurrentEdge) && (value == valueReverseCurrentEdge))) {
						remove.add(value);
					}
				}
			}
		}
		// se entrar no if anterior, remove algo, caso contrário, passa direto.
		String[] str = t._2().split(" ");
		TreeSet<Integer> set = new TreeSet<Integer>();
		for(int i = 0; i < str.length; i++){
			set.add(Integer.parseInt(str[i]));
		}
		while (!remove.isEmpty()) {
			set.remove(remove.remove());
		}
		StringBuilder builder = new StringBuilder();
		for(int i : set){
			builder.append(i + " ");
		}
		return new Tuple2(t._1(), builder.toString());
	}

	public int[][] getAffectedVertices() {
		return affectedVertices;
	}

	public void setAffectedVertices(int[][] affectedVertices) {
		this.affectedVertices = affectedVertices;
	}

}
