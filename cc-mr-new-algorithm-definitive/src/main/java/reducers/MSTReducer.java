package reducers;

import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.function.Function2;

public class MSTReducer implements Function2<String, String, String> {

	private static final long serialVersionUID = 1L;

	public MSTReducer() {

	}

	public String call(String vsrc, String vdest) throws Exception {
		// vsrc.addAll(vdest);
		TreeSet<Integer> set = new TreeSet<Integer>();
		String[] s1 = vsrc.split(" ");
		String[] s2 = vdest.split(" ");
		for (int i = 0; i < s1.length; i++) {
			set.add(Integer.parseInt(s1[i]));
		}
		for (int i = 0; i < s2.length; i++) {
			set.add(Integer.parseInt(s2[i]));
		}
		StringBuilder result = new StringBuilder();
		for (int num : set) {
			result.append(num + " ");
		}
		return result.toString();
	}
}
