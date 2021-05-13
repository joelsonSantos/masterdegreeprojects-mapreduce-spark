package reducers;

import org.apache.spark.api.java.function.Function2;

import java.util.TreeSet;

public class ConstructADJReducer implements Function2<String, String, String> {

	private static final long serialVersionUID = 1L;

	public String call(String v1, String v2) throws Exception {
		TreeSet<Integer> set = new TreeSet<Integer>();
		String[] s1 = v1.split(" ");
		String[] s2 = v2.split(" ");
		for (int i = 0; i < s1.length; i++) {
          set.add(Integer.parseInt(s1[i]));
		}
		for (int i = 0; i < s2.length; i++) {
			 set.add(Integer.parseInt(s2[i]));
		}
		StringBuilder result = new StringBuilder();
		for(int num : set){
			result.append(num + " ");
		}
		return result.toString();
	}
}
