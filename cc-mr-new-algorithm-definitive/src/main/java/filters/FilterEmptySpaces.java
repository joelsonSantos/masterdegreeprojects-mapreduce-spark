package filters;

import org.apache.spark.api.java.function.Function;

public class FilterEmptySpaces implements Function<String, Boolean> {

	private static final long serialVersionUID = 1L;

	public Boolean call(String v1) throws Exception {
		String[] s = v1.split(" ");
		return s.length > 1;
	}
}
