package mappers;

import org.apache.spark.api.java.function.Function;

public class InversoMapper implements Function<String, String> {

	private static final long serialVersionUID = 1L;

	public String call(String v1) throws Exception {
		String[] s = v1.split(" ");
		StringBuilder string = new StringBuilder();
		if (s.length > 1) {
			string.append(s[1] + " " + s[0] + " " + s[2]);
		}
		return string.toString();
	}
}
