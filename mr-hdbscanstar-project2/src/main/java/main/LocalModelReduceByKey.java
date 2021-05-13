package main;

import java.util.Map;

import org.apache.spark.api.java.function.Function2;

import databubbles.HdbscanDataBubbles;
import scala.Tuple3;

public class LocalModelReduceByKey implements
		Function2<Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>, Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>, Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>> {

	private static final long serialVersionUID = 1L;
	private Integer mpts;
	private Integer mclSize;
	private Map<Integer, Integer> key;

	public LocalModelReduceByKey(Integer mpts, Integer mclSize, Map<Integer, Integer> key) {
		this.mpts = mpts;
		this.mclSize = mclSize;
		this.key = key;
	}

	public Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>> call(
			Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>> v1,
			Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>> v2)
			throws Exception {
		int[][] flatPartition = null;
		int count = 0;
		int id = v1._2()._3().intValue();
		int size = 0;
		for (Integer key : this.key.keySet()) {
			if (id == key.intValue()) {
				size = this.key.get(key).intValue();
			}
		}
		double[][] data = new double[v1._1()._1().length + v2._1()._1().length][v2._1()._1()[0].length];
		double[][] infoBubbles = new double[v1._1()._2().length + v2._1()._2().length][v2._1()._2()[0].length];
		int[] idBubbles = new int[v1._1()._3().length + v2._1()._3().length];

		for (int i = 0; i < v1._1()._1().length; i++) {
			data[i] = v1._1()._1()[i];
			count++;
		}
		for (int i = 0; i < v2._1()._1().length; i++) {
			data[count++] = v2._1()._1()[i];
		}
		count = 0;
		for (int i = 0; i < v1._1()._2().length; i++) {
			infoBubbles[i] = v1._1()._2()[i];
			count++;
		}
		for (int i = 0; i < v2._1()._2().length; i++) {
			infoBubbles[count++] = v2._1()._2()[i];
		}

		count = 0;
		for (int i = 0; i < v1._1()._3().length; i++) {
			idBubbles[i] = v1._1()._3()[i];
			count++;
		}
		for (int i = 0; i < v2._1()._3().length; i++) {
			idBubbles[count++] = v2._1()._3()[i];
		}

		double[] eB = new double[infoBubbles.length];
		double[] nnDistB = new double[infoBubbles.length];
		int[] nB = new int[infoBubbles.length];

		for (int i = 0; i < eB.length; i++) {
			eB[i] = infoBubbles[i][0];
			nnDistB[i] = infoBubbles[i][1];
			nB[i] = (int) infoBubbles[i][2];
		}
		// computing hierarchy;
		if (nB.length >= size) {
			HdbscanDataBubbles model = new HdbscanDataBubbles();
			double[] coreDistances = model.calculateCoreDistancesBubbles(data, nB, eB, nnDistB, this.mpts);
			UndirectedGraph mst = model.constructMSTBubbles(data, nB, eB, nnDistB, idBubbles, coreDistances, true);
			mst.quicksortByEdgeWeight();
			int maxVertexLabel = 0;
			for (int i = 0; i < mst.getNumEdges(); i++) {
				if (maxVertexLabel < mst.getFirstVertexAtIndex(i)) {
					maxVertexLabel = mst.getFirstVertexAtIndex(i);
				}
				if (maxVertexLabel < mst.getSecondVertexAtIndex(i)) {
					maxVertexLabel = mst.getSecondVertexAtIndex(i);
				}
			}
			model.constructClusterTree(mst, this.mclSize, nB, maxVertexLabel);
			flatPartition = model.findProminentClustersAndClassificationNoiseBubbles(model.getClusters(), data, nB, eB,
					nnDistB, idBubbles);
			model.findInterClusterEdges(mst, flatPartition);
			int[] objects = new int[flatPartition.length];
			int[] labels = new int[flatPartition.length];
			for (int i = 0; i < labels.length; i++) {
				objects[i] = flatPartition[i][0];
				labels[i] = flatPartition[i][1];
			}
			// <dataset bubbles, data bubbles information>, <flat partition>, <mst> 
			return new Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>(
					new Tuple3<double[][], double[][], int[]>(data, infoBubbles, idBubbles),
					new Tuple3<int[], int[], Integer>(objects, labels, id),
					new Tuple3<int[], int[], double[]>(model.getVertice1(), model.getVertice2(), model.getDmreach()));
		} else {
			return new Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>(
					new Tuple3<double[][], double[][], int[]>(data, infoBubbles, idBubbles),
					new Tuple3<int[], int[], Integer>(null, null, id), null);
		}
	}

}