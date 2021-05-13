package mappers;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SizeComponenteMapper
		implements PairFunction<String, Integer, String> {
	private static final long serialVersionUID = 1L;
	private int[][] affectedVertices;

	public SizeComponenteMapper(int[][] affectedVertices) {
		this.setAffectedVertices(affectedVertices);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<Integer, String> call(String t) throws Exception {
		String[] s = t.split(" ");
		String size = null;
		if (s.length > 2) {
			size = s[2];
		}
		boolean has = false;
		for (int i = 0; i < affectedVertices.length; i++) {
			if (affectedVertices[i][1] == Integer.parseInt(s[1])) {
               has = true;
			}
		}
		return new Tuple2(Integer.parseInt(s[0]), size + " " + has);
	}

	public int[][] getAffectedVertices() {
		return affectedVertices;
	}

	public void setAffectedVertices(int[][] affectedVertices) {
		this.affectedVertices = affectedVertices;
	}
}
