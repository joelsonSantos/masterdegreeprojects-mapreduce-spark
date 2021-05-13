package flatmappers;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class CCMRMapper implements
		FlatMapFunction<Tuple2<Integer, String>, String> {

	private static final long serialVersionUID = 1L;
    private Accumulator<Integer> accum;
	
	public CCMRMapper(Accumulator<Integer> accum) {
		//Main.newIterationNeeded = false;
		this.accum = accum;
	}

	public Iterable<String> call(Tuple2<Integer, String> t)
			throws Exception {

		StringBuilder sReturned = new StringBuilder();
		//NewList result = new NewList();
		
		boolean isLocalMaxState = false;
		String[] adjacents = t._2().split(" ");
		int vFirst = Integer.parseInt(adjacents[0]);
		int vSource = t._1().intValue();

		if (vSource <= vFirst) {
			isLocalMaxState = true;
			sReturned.append(vSource + " " + vFirst + " " + 1);
			//result.add(vSource + " " + vFirst + " " + 1);
		}

		int vDestOld = vFirst;
		int vDest = -1;
		//Iterator<Integer> iterator = t._2().iterator();
		for(int i = 0; i < adjacents.length; i++) {
			vDest = Integer.parseInt(adjacents[i]);
			if (vDest == vDestOld) {
				continue;
			}
			if (isLocalMaxState) {
				// key : (Ci) - value: vDest , currentLevel, stability, sizeCC,
				// offsetLevel, outlierScore
				sReturned.append("\n" + vSource + " " + vDest + " " + 1);
				//result.add(vSource + " " + vDest + " " + 1);
			} else {
				 sReturned.append("\n" + vFirst + " " + vDest + "\n" + vDest + " " + vFirst);
				//result.add(vFirst + " " + vDest);
				//result.add(vDest + " " + vFirst);
				// Main.newIterationNeeded = true;
				accum.add(1);
			}
			vDestOld = vDest;
		}
		// stdMergeCase
		if (vSource < vDest && vDest != -1 && !isLocalMaxState) {
			sReturned.append("\n" + vSource + " " + vFirst);
			//result.add(vSource + " " + vFirst);
		}
		
		return Arrays.asList(sReturned.toString());
		//return Arrays.asList(result.toString());
	}
}
