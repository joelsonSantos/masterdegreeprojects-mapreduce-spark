package main;

import org.apache.spark.api.java.function.Function2;

public class UpdateStability implements Function2<int[], int[], int[]> {

	private static final long serialVersionUID = 1L;
	private int minClusterSize;
	private int oldLabel;
	
	public UpdateStability(int m, int oldLabel) {
	  this.minClusterSize = m;	
	  this.oldLabel = oldLabel;
	}
	@Override
	public int[] call(int[] element1, int[] element2) throws Exception {
		if((element1.length >= this.minClusterSize) && (element2.length >= this.minClusterSize)) {
			
		}
		return null;
	}
}
