package main;

import org.apache.spark.api.java.function.Function2;

public class RemakeAdjacentList implements Function2<int[], int[], int[]> {

	private static final long serialVersionUID = 1L;
	private int vertex1;
	private int vertex2;

	public RemakeAdjacentList(int vertex1, int vertex2) {
		this.vertex1 = vertex1;
		this.vertex2 = vertex2;
	}

	// linear
	public int[] call(int[] v1, int[] v2) throws Exception {
		int[] newArray = new int[v1.length + v2.length];
		int aux = 0;
		int index = 0;
		boolean verify = false;
		for (int i = 0; i < v1.length; i++) {
			if ((v1[i] == this.vertex1) || (v1[i] == this.vertex2)) {
				aux = v1[i];
				index = i;
				verify = true;
			}
			newArray[i] = v1[i];
		}
		for (int i = 0; i < v2.length; i++) {
			if ((v2[i] == this.vertex1) || (v2[i] == this.vertex2)) {
				aux = v2[i];
				index = v1.length + i;
				verify = true;
			}
			newArray[v1.length + i] = v2[i];
		}
		if (verify) {
			int aux2 = newArray[index];
			newArray[index] = newArray[0];
			newArray[0] = aux2;
		}
		return newArray;
	}

}
