package mappers;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.spark.api.java.function.PairFunction;

import extra.methods.NewList;
import main.Main;
import scala.Tuple2;

public class TestMapper
		implements PairFunction<Tuple2<Integer, TreeSet<Integer>>, String, String> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple2<String, String> call(Tuple2<Integer, TreeSet<Integer>> t) throws Exception {
		
		NewList result = new NewList();
		
		boolean isLocalMaxState = false;
		int vFirst = t._2().first();
		int vSource = t._1().intValue();

		if (vSource <= vFirst) {
			isLocalMaxState = true;
			result.add(vSource + " " + vFirst + " " + 1);
		}

		int vDestOld = vFirst;
		int vDest = -1;
		Iterator<Integer> iterator = t._2().iterator();
		while (iterator.hasNext()) {
			vDest = iterator.next();
			if (vDest == vDestOld) {
				continue;
			}
			if (isLocalMaxState) {
				// key : (Ci) - value: vDest , currentLevel, stability, sizeCC,
				// offsetLevel, outlierScore
				result.add(vSource + " " + vDest + " " + 1);
			} else {
				result.add(vFirst + " " + vDest);
				result.add(vDest + " " + vFirst);
				Main.newIterationNeeded = true;
			}
			vDestOld = vDest;
		}
		// stdMergeCase
		if (vSource < vDest && vDest != -1 && !isLocalMaxState) {
			result.add(vSource + " " + vFirst);
		}
		
		return new Tuple2(1, result.toString());
	}
}
