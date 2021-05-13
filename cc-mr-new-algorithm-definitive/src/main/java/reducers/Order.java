package reducers;

import java.util.Collections;

import org.apache.spark.api.java.function.Function2;

import extra.methods.MinimumSpanningTree;
import extra.methods.Sort;

public class Order
		implements
		Function2<MinimumSpanningTree, MinimumSpanningTree, MinimumSpanningTree> {

	private static final long serialVersionUID = 1L;

	public MinimumSpanningTree call(MinimumSpanningTree v1,
			MinimumSpanningTree v2) throws Exception {
		v1.getListMst().addAll(v2.getListMst());
		Sort sort = new Sort();
		Collections.sort(v1.getListMst(), sort);
		return v1;
	}
}
