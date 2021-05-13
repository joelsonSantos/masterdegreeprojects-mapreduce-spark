package filters;

import org.apache.spark.api.java.function.Function;

public class FilterMaxEdge implements Function<String, Boolean> {

	private static final long serialVersionUID = 1L;
	private String edge;

	public FilterMaxEdge(String edge) {
		this.edge = edge;
	}

	public Boolean call(String v1) throws Exception {
		return !v1.equals(this.edge);
	}
}
