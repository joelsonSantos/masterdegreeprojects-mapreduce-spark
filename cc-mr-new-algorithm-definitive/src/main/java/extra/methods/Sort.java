package extra.methods;

import java.io.Serializable;
import java.util.Comparator;

import extra.methods.MinimumSpanningTree;

public class Sort implements Comparator<MinimumSpanningTree>, Serializable {
	private static final long serialVersionUID = 1L;

	public int compare(MinimumSpanningTree v1, MinimumSpanningTree v2) {
		if(v1.getWeight() < v2.getWeight()){
			return 1;
		}
		else if(v1.getWeight() > v2.getWeight()){
			return -1;
		}
		return 0;
	}
}