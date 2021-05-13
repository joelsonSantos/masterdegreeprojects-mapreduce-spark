package filters;

import org.apache.spark.api.java.function.Function;

public class FilterDuplicates implements Function<String, Boolean> {
	private static final long serialVersionUID = 1L;

	public Boolean call(String v1) throws Exception {
		String[] s = v1.split(" ");
		int v = Integer.parseInt(s[0]);
		int u = Integer.parseInt(s[1]);
		return v <= u;
	}

}
