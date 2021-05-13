package reducers;

import org.apache.spark.api.java.function.Function2;

public class SizeComponenteReducer
		implements Function2<String, String, String> {

	private static final long serialVersionUID = 1L;
	
	public String call(String v1, String v2) throws Exception {
		String[] s1 = v1.split(" ");
		String[] s2 = v2.split(" ");
		int size1 = Integer.parseInt(s1[0]);
		int size2 = Integer.parseInt(s2[0]);
		boolean condition1 = Boolean.parseBoolean(s1[1]);
		boolean condition2 = Boolean.parseBoolean(s2[1]);
		int newSize = size1 + size2;
		boolean has = false;
		   if(condition1){
			   has = condition1;
		   }else if(condition2){
			   has = condition2;
		   }
		String result = newSize + " " + has;
		return result;
	}
}
