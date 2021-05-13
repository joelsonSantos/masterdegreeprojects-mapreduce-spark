package main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import distance.DistanceCalculator;
import distance.EuclideanDistance;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;
import scala.Tuple7;
import scala.Tuple8;

public final class Main {
	private static final String FILE_FLAG = "file=";
	private static final String CLUSTERNAME_FLAG = "clusterName=";
	private static final String MIN_PTS_FLAG = "minPts=";
	private static final String K_FLAG = "k=";
	private static final String PROCESSING_UNITS = "processing_units=";
	private static final String MIN_CL_SIZE_FLAG = "minClSize=";
	private static final String COMPACT_FLAG = "compact=";
	private static final String DISTANCE_FUNCTION_FLAG = "dist_function=";

	private static final String EUCLIDEAN_DISTANCE = "euclidean";
	private static final String COSINE_SIMILARITY = "cosine";
	private static final String PEARSON_CORRELATION = "pearson";
	private static final String MANHATTAN_DISTANCE = "manhattan";
	private static final String SUPREMUM_DISTANCE = "supremum";

	private static final int FILE_BUFFER_SIZE = 32678;

	private static SparkConf conf;
	private static JavaRDD<String> dataFile;
	private static JavaPairRDD<Integer, Tuple2<Integer, double[]>> dataset;
	// public static int nodeCount = 0;
	public static LongAccumulator newIteration;

	public static void main(String[] args) throws IOException {
		
		// /usr/local/spark/bin/spark-submit --class spark.scala.Main --master spark://IP:7077 /usr/local/spark/jars/PDHDBSCANStar1.0.jar file=/caminho_dataset/base minPts=4 minClSize=4 processing_units=50 k=20 clusterName=local
		
		// Parse input parameters from program arguments:
		HDBSCANStarParameters parameters = checkInputParameters(args);

		System.out.println("Running MR-HDBSCAN* on " + parameters.inputFile + " with minPts=" + parameters.minPoints
				+ ", minClSize=" + parameters.minClusterSize + ", dist_function=euclidean distance"
				+ ", processing_units=" + parameters.processing_units + ", k=" + parameters.k + ", clusterName="
				+ parameters.clusterName);

		// Logger.getLogger("org").setLevel(Level.OFF);
		// Logger.getLogger("akka").setLevel(Level.OFF);

		// Creating a JavaSparkContext
		String inputName = parameters.inputFile;
		if (parameters.inputFile.contains(".")) {
			inputName = parameters.inputFile.substring(0, parameters.inputFile.lastIndexOf("."));
		}
		conf = new SparkConf().setAppName("MR-HDBSCAN* on dataset: " + inputName).setMaster(parameters.clusterName) // spark://master:7077
				.set("spark.hadoop.validateOutputSpecs", "false");
		/*
		 * .set("spark.driver.cores", "6") .set("spark.driver.memory", "6g")
		 * .set("spark.executor.memory", "6g");
		 */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		dataFile = jsc.textFile(parameters.inputFile);
		System.out.println("Running on: " + parameters.inputFile);
		// reading dataset and saving each object as a pair <id, object as double[]>
		dataset = dataFile.mapToPair(new PairFunction<String, Integer, Tuple2<Integer, double[]>>() {
			private static final long serialVersionUID = 1L;
			private int count = -1;

			@SuppressWarnings({ "unchecked", "rawtypes" })
			public Tuple2<Integer, Tuple2<Integer, double[]>> call(String s) throws Exception {
				String[] sarray = s.split(" ");
				double[] dataPoint = new double[sarray.length];
				for (int i = 0; i < sarray.length; i++) {
					dataPoint[i] = Double.parseDouble(sarray[i]);
				}
				count++;
				return new Tuple2(0, new Tuple2(count, dataPoint));
			}
		}).cache();
		dataset.coalesce(1, true).saveAsObjectFile(parameters.inputFile + "_unprocessed_0");
		int iteration = 0, nextLevel = 0, processedPointsCounter = 0, splitByKey = 1;
		int datasetSize = (int) dataset.count();
		int nextIdSubsets = 2, nextClusterLabel = 2;

		List<Tuple8<Integer, int[], int[], int[], double[], double[], Integer, Integer>> elem = new ArrayList<Tuple8<Integer, int[], int[], int[], double[], double[], Integer, Integer>>();
		int[] dat = new int[datasetSize];
		int[] clusterParentLabels = new int[datasetSize * 2];
		int[] clusterChildrenLabels = new int[datasetSize * 2];
		double[] clusterStability = new double[datasetSize * 2];
		double[] birthLevel = new double[datasetSize * 2];
		for (int object = 0; object < dat.length; object++) {
			dat[object] = 1;
		}
		for (int object = 0; object < clusterParentLabels.length; object++) {
			clusterParentLabels[object] = -1;
			clusterStability[object] = 0;
			birthLevel[object] = Double.MAX_VALUE;
			clusterChildrenLabels[object] = -1;
		}
		// key -> labels, clusters parent labels, cluster children labels, stability of
		// clusters on tree, birth level, next value
		// label for new
		// clusters, last element position on tree (array)
		elem.add(new Tuple8<Integer, int[], int[], int[], double[], double[], Integer, Integer>(1, dat,
				clusterParentLabels, clusterChildrenLabels, clusterStability, birthLevel, nextClusterLabel, 0));

		JavaPairRDD<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>> distData = jsc
				.parallelize(elem).mapToPair(
						new PairFunction<Tuple8<Integer, int[], int[], int[], double[], double[], Integer, Integer>, Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>>() {
							private static final long serialVersionUID = 1L;

							@Override
							// key -> labels, clusters parent labels, cluster children labels, stability of
							// clusters on tree, birth level, next value
							// label for new
							// clusters, last element position on tree (array)
							public Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>> call(
									Tuple8<Integer, int[], int[], int[], double[], double[], Integer, Integer> arg0)
									throws Exception {
								return new Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>>(
										arg0._1(),
										new Tuple7<int[], int[], int[], double[], double[], Integer, Integer>(arg0._2(),
												arg0._3(), arg0._4(), arg0._5(), arg0._6(), arg0._7(), arg0._8()));
							}
						});
		distData.saveAsObjectFile(parameters.inputFile + "_level_" + 0);
		elem.clear();
		////////////////////////////////////////////////////////////////////////////////////////////////

		long start = System.currentTimeMillis();
		// first step of MR-HDBSCAN* (using a mapPartitionsToPair)
		while (processedPointsCounter < datasetSize) {
			JavaRDD<Tuple2<Integer, Tuple2<Integer, double[]>>> data;
			data = jsc.objectFile(parameters.inputFile + "_unprocessed_" + iteration);
			// compute the size of each subset and save the number of attributes
			// of each object using the keys
			List<Tuple2<Integer, Tuple2<Integer, Integer>>> keyCounts = JavaPairRDD.fromJavaRDD(data).mapToPair(
					new PairFunction<Tuple2<Integer, Tuple2<Integer, double[]>>, Integer, Tuple2<Integer, Integer>>() {
						private static final long serialVersionUID = 1L;

						public Tuple2<Integer, Tuple2<Integer, Integer>> call(
								Tuple2<Integer, Tuple2<Integer, double[]>> t) throws Exception {
							return new Tuple2<Integer, Tuple2<Integer, Integer>>(t._1(),
									new Tuple2<Integer, Integer>(1, t._2()._2().length));
						}
					}).reduceByKey(
							new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
								private static final long serialVersionUID = 1L;

								public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1,
										Tuple2<Integer, Integer> v2) throws Exception {
									return new Tuple2<Integer, Integer>(v1._1().intValue() + v2._1().intValue(),
											v1._2());
								}
							})
					.collect();
			Map<Integer, Double> fractions = new HashMap<Integer, Double>();
			for (int i = 0; i < keyCounts.size(); i++) {
				fractions.put(keyCounts.get(i)._1(), parameters.k);
				if (keyCounts.get(i)._2()._1().intValue() <= parameters.processing_units) {
					processedPointsCounter += keyCounts.get(i)._2()._1().intValue();
				}
			}
			// selecting stratified sampling from multiple subsets
			List<Tuple2<Integer, Tuple2<Integer, double[]>>> s = JavaPairRDD.fromJavaRDD(data)
					.sampleByKeyExact(false, fractions).collect();

			List<Tuple2<Integer, Tuple2<Integer, double[]>>> samples = new ArrayList<Tuple2<Integer, Tuple2<Integer, double[]>>>();

			for (int i = 0; i < keyCounts.size(); i++) {
				int count = 0;
				for (int j = 0; j < s.size(); j++) {
					if (keyCounts.get(i)._1().intValue() == s.get(j)._1().intValue()) {
						samples.add(new Tuple2<Integer, Tuple2<Integer, double[]>>(s.get(j)._1(),
								new Tuple2<Integer, double[]>(count, s.get(j)._2()._2())));
						count++;
					}
				}
			}
			Map<Integer, Integer> countSamplesByKey = new HashMap<Integer, Integer>();
			for (int i = 0; i < samples.size(); i++) {
				if (countSamplesByKey.get(samples.get(i)._1().intValue()) == null) {
					countSamplesByKey.put(samples.get(i)._1().intValue(), 0);
				}
				int value = countSamplesByKey.get(samples.get(i)._1().intValue());
				countSamplesByKey.put(samples.get(i)._1().intValue(), value + 1);
			}

			// doing the first step of MR-HDBSCAN*
			JavaPairRDD<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>> subsets = JavaPairRDD
					.fromJavaRDD(data).mapPartitionsToPair(new FirstStep(parameters.k, parameters.processing_units,
							parameters.minPoints, parameters.inputFile, iteration, samples, keyCounts));

			// mapping the edges...
			JavaPairRDD<Integer, Tuple3<Integer, Integer, Double>> mst = subsets.filter(
					new Function<Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>>, Boolean>() {
						private static final long serialVersionUID = 1L;

						public Boolean call(
								Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>> arg0)
								throws Exception {
							return arg0._1().intValue() == -1;
						}
					}).flatMapToPair(
							new PairFlatMapFunction<Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>>, Integer, Tuple3<Integer, Integer, Double>>() {
								private static final long serialVersionUID = 1L;

								public Iterator<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> call(
										Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>> t)
										throws Exception {
									ArrayList<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> mst = new ArrayList<Tuple2<Integer, Tuple3<Integer, Integer, Double>>>();
									int v = t._2()._1();
									int u = t._2()._3();
									double dmreach = t._2()._2()._1()[0];
									// 1, v, dmreach, u
									mst.add(new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(-1,
											new Tuple3<Integer, Integer, Double>(v, u, dmreach)));
									return mst.iterator();
								}
							});
			mst.saveAsObjectFile(parameters.inputFile + "_local_mst" + iteration);
			iteration++;
			if (processedPointsCounter < datasetSize) {
				subsets = subsets.filter(
						new Function<Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>>, Boolean>() {
							private static final long serialVersionUID = 1L;

							public Boolean call(
									Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>> arg0)
									throws Exception {
								return arg0._1().intValue() != -1;
							}
						});
				JavaPairRDD<Integer, Tuple3<Integer, Integer, double[]>> tes = subsets.mapToPair( // nearest,
						// id,
						// idobject,
						// object
						new PairFunction<Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>>, Integer, Tuple3<Integer, Integer, double[]>>() {
							private static final long serialVersionUID = 1L;

							public Tuple2<Integer, Tuple3<Integer, Integer, double[]>> call(
									Tuple2<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>> t)
									throws Exception {
								// nearest, <id, idObject, object>
								return new Tuple2<Integer, Tuple3<Integer, Integer, double[]>>(t._1(),
										new Tuple3<Integer, Integer, double[]>(t._2()._3(), t._2()._1(),
												t._2()._2()._1()));
							}

						});
				tes.saveAsObjectFile(parameters.inputFile + "_nearest_" + iteration);
				// local and global combining (creating Data Bubbles);
				JavaPairRDD<Integer, Tuple3<Integer, Tuple4<double[], double[], double[], double[]>, Integer>> dataBubbles = subsets
						.reduceByKey(new CombineStep());
				dataBubbles.saveAsTextFile(parameters.inputFile + "_data_bubbles_" + iteration);

				// HDBSCAN* hierarchy from data bubbles
				// id, bubbles, partition from bubbles, inter-cluster edges;
				JavaPairRDD<Integer, Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>> localModel = dataBubbles
						.mapToPair(new ReMapBubbles()).reduceByKey(new LocalModelReduceByKey(parameters.minPoints,
								parameters.minClusterSize, countSamplesByKey));

				// filtering and persisting the inter-cluster edges
				localModel.flatMapToPair(
						new PairFlatMapFunction<Tuple2<Integer, Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>>, Integer, Tuple3<Integer, Integer, Double>>() {
							private static final long serialVersionUID = 1L;

							public Iterator<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> call(
									Tuple2<Integer, Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>> t)
									throws Exception {
								ArrayList<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> mst = new ArrayList<Tuple2<Integer, Tuple3<Integer, Integer, Double>>>();
								if (t._2()._3() != null) {
									// 1, v, dmreach, u
									if (t._2()._3()._2() != null)
										mst.add(new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(-1,
												new Tuple3<Integer, Integer, Double>(t._2()._3()._1()[0],
														t._2()._3()._2()[0], t._2()._3()._3()[0])));
								}
								return mst.iterator();
							}
						}).saveAsObjectFile(parameters.inputFile + "_local_mst_" + iteration);

				List<Tuple2<Integer, Tuple3<Tuple3<double[][], double[][], int[]>, Tuple3<int[], int[], Integer>, Tuple3<int[], int[], double[]>>>> model = localModel
						.collect();
				// data partition induction
				splitByKey = 0;
				TreeSet<Integer> newIds = new TreeSet<Integer>();
				for (int i = 0; i < model.size(); i++) {
					if (model.get(i)._2()._2()._1() != null) {
						for (int member = 0; member < model.get(i)._2()._2()._1().length; member++) {
							newIds.add(model.get(i)._2()._2()._2()[member]);
						}
						splitByKey += newIds.size();
						while (!newIds.isEmpty()) {
							int clusterId = newIds.pollFirst();
							for (int member = 0; member < model.get(i)._2()._2()._1().length; member++) {
								if (model.get(i)._2()._2()._2()[member] == clusterId) {
									model.get(i)._2()._2()._2()[member] = nextIdSubsets;
								}
							}
							nextIdSubsets++;
						}
					}
				}
				splitByKey = (splitByKey > 0) ? splitByKey : 1;
				// reading subsets
				JavaRDD<Tuple2<Integer, Tuple3<Integer, Integer, double[]>>> data1 = jsc
						.objectFile(parameters.inputFile + "_nearest_" + iteration);

				// JavaPairRDD<Integer, Tuple2<Integer, double[]>> e =
				JavaPairRDD.fromJavaRDD(data1).mapToPair(new LabelClassification(model))
						.partitionBy(new HashPartitioner(splitByKey))
						.saveAsObjectFile(parameters.inputFile + "_unprocessed_" + iteration);
			}
		}
		BufferedWriter time = new BufferedWriter(new FileWriter("time_recursive_sampling.txt"), FILE_BUFFER_SIZE);
		long end = System.currentTimeMillis();
		time.write("Time without hierarchy : " + ((end - start) / 1000) + "\n");
		time.close();
		// second step of MR-HDBSCAN*
		// merging and sorting the local MSTs in an unique solution;
		JavaRDD<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> MSTFiles = jsc
				.objectFile(parameters.inputFile + "_local_mst*");
		int numberOfEdges = ((datasetSize * 2) - 1);
		if (numberOfEdges <= parameters.processing_units) {
			List<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> mst = JavaPairRDD.fromJavaRDD(MSTFiles).mapToPair(
					new PairFunction<Tuple2<Integer, Tuple3<Integer, Integer, Double>>, Integer, Tuple3<Integer, Integer, Double>>() {
						private static final long serialVersionUID = 1L;

						public Tuple2<Integer, Tuple3<Integer, Integer, Double>> call(
								Tuple2<Integer, Tuple3<Integer, Integer, Double>> t) throws Exception {
							return new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(1,
									new Tuple3<Integer, Integer, Double>(t._2()._1(), t._2()._2(), t._2()._3()));
						}
					}).collect();
			int[] verticeOne = new int[numberOfEdges];
			int[] verticeTwo = new int[numberOfEdges];
			double[] weight = new double[numberOfEdges];
			int index = 0;
			Iterator<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> it = mst.iterator();
			while (it.hasNext()) {
				Tuple2<Integer, Tuple3<Integer, Integer, Double>> element = it.next();
				verticeOne[index] = element._2()._1();
				verticeTwo[index] = element._2()._2();
				weight[index] = element._2()._3();
				index++;
			}
			mst = null;
			HDBSCANStar model = new HDBSCANStar();
			double[] pointNoiseLevels = new double[datasetSize];
			int[] pointLastClusters = new int[datasetSize];
			UndirectedGraph graph = new UndirectedGraph(datasetSize, verticeOne, verticeTwo, weight);
			graph.quicksortByEdgeWeight();
			ArrayList<Cluster> clusters = model.computeHierarchyAndClusterTree(graph, parameters.minClusterSize, true,
					null, parameters.hierarchyFile, parameters.clusterTreeFile, ",", pointNoiseLevels,
					pointLastClusters);
			// Remove references to unneeded objects:
			graph = null;
			// Propagate clusters:
			boolean infiniteStability = model.propagateTree(clusters);
			model.findProminentClusters(clusters, parameters.hierarchyFile, parameters.partitionFile, ",", datasetSize,
					infiniteStability);
		} else {
			JavaPairRDD.fromJavaRDD(MSTFiles).flatMapToPair(
					new PairFlatMapFunction<Tuple2<Integer, Tuple3<Integer, Integer, Double>>, Integer, int[]>() {
						private static final long serialVersionUID = 1L;

						public Iterator<Tuple2<Integer, int[]>> call(
								Tuple2<Integer, Tuple3<Integer, Integer, Double>> t) throws Exception {
							ArrayList<Tuple2<Integer, int[]>> list = new ArrayList<Tuple2<Integer, int[]>>();
							int[] v = { t._2()._2() };
							int[] u = { t._2()._1() };
							list.add(new Tuple2<Integer, int[]>(t._2()._1(), v));
							list.add(new Tuple2<Integer, int[]>(t._2()._2(), u));
							return list.iterator();
						}
					}).reduceByKey(new Function2<int[], int[], int[]>() {
						private static final long serialVersionUID = 1L;

						public int[] call(int[] v1, int[] v2) throws Exception {
							int[] adj = new int[v1.length + v2.length];
							int count = 0;
							for (int i = 0; i < v1.length; i++) {
								adj[count] = v1[i];
								count++;
							}
							for (int i = 0; i < v2.length; i++) {
								adj[count] = v2[i];
								count++;
							}
							return adj;
						}
					}).saveAsObjectFile(parameters.inputFile + "_adjList");
			while (numberOfEdges > 0) {
				JavaPairRDD<Integer, Tuple3<Integer, Integer, Double>> m = JavaPairRDD.fromJavaRDD(MSTFiles).mapToPair(
						new PairFunction<Tuple2<Integer, Tuple3<Integer, Integer, Double>>, Integer, Tuple3<Integer, Integer, Double>>() {
							private static final long serialVersionUID = 1L;

							public Tuple2<Integer, Tuple3<Integer, Integer, Double>> call(
									Tuple2<Integer, Tuple3<Integer, Integer, Double>> t) throws Exception {
								return new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(t._1(),
										new Tuple3<Integer, Integer, Double>(t._2()._1(), t._2()._2(), t._2()._3()));
							}
						});
				// retrieve highest edges on current hierarchy level
				List<Tuple2<Integer, Tuple3<Integer, Integer, Double>>> highestEdgeWeight = m.reduceByKey(
						new Function2<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>>() {
							private static final long serialVersionUID = 1L;

							public Tuple3<Integer, Integer, Double> call(Tuple3<Integer, Integer, Double> v1,
									Tuple3<Integer, Integer, Double> v2) throws Exception {
								return (v1._3() >= v2._3()) ? v1 : v2;
							}
						}).collect();
				// reading last level
				JavaRDD<Tuple2<Integer, Tuple7<int[], int[], int[], double[], double[], Integer, Integer>>> lastLevelFile = jsc
						.objectFile(parameters.inputFile + "_level_" + nextLevel);
				List<Integer> oldLabels = JavaPairRDD.fromJavaRDD(lastLevelFile)
						.flatMap(new LastLabels(highestEdgeWeight.get(0)._2()._2().intValue())).collect();
				JavaRDD<Tuple2<Integer, int[]>> adj = jsc.objectFile(parameters.inputFile + "_adjList");
				JavaPairRDD<Integer, int[]> adjList = JavaPairRDD.fromJavaRDD(adj)
						.mapToPair(new FilterAdjacentVertex(highestEdgeWeight));
				// saving changes on adjacent list
				adjList.saveAsObjectFile(parameters.inputFile + "_adjList");
				m.filter(new FilterHighestEdgeWeight(highestEdgeWeight))
						.saveAsObjectFile(parameters.inputFile + "_newMST");
				MSTFiles = jsc.objectFile(parameters.inputFile + "_newMST");
				// finding connected components (clusters)
				JavaPairRDD<Integer, int[]> subcomponents = adjList;
				do {
					newIteration = jsc.sc().longAccumulator(); // init with 0
					subcomponents = subcomponents.flatMapToPair(new FindConnectedComponentsOnMST2())
							.reduceByKey(new RemakeAdjacentList(highestEdgeWeight.get(0)._2()._1(),
									highestEdgeWeight.get(0)._2()._2()));
					subcomponents.saveAsObjectFile(parameters.inputFile + "_clusters_");
				} while (newIteration.value() != 0);
				int label = oldLabels.get(0).intValue();
				double weight = highestEdgeWeight.get(0)._2()._3();
				// read current components (clusters)
				JavaRDD<Tuple2<Integer, int[]>> clustersFile = jsc
						.objectFile(parameters.inputFile + "_clusters_");
				Map<Integer, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>> clusters = JavaPairRDD
						.fromJavaRDD(clustersFile)
						.filter(new FilterNoTrueComponents(highestEdgeWeight.get(0)._2()._1(),
								highestEdgeWeight.get(0)._2()._2()))
						.mapToPair(new ConstructHierarchyLevel(label))
						.reduceByKey(new LabelingClusters(label, parameters.minClusterSize)).collectAsMap();
				Broadcast<Map<Integer, Tuple2<Tuple3<Integer, int[], Boolean>, Tuple3<Integer, int[], Boolean>>>> bd = jsc
						.broadcast(clusters);
				nextLevel++;
				// re-labeling clusters as new valid or noise.
				JavaPairRDD.fromJavaRDD(lastLevelFile)
						.mapToPair(
								new ConstructNewHierarchicalLevel(clusters, label, weight, parameters.minClusterSize))
						.saveAsObjectFile(parameters.inputFile + "_level_" + nextLevel);
				lastLevelFile = jsc.objectFile(parameters.inputFile + "_level_" + nextLevel);
			}
		}
	}

	/* ANOTHER METHODS */
	private static HDBSCANStarParameters checkInputParameters(String[] args) {
		HDBSCANStarParameters parameters = new HDBSCANStarParameters();
		parameters.distanceFunction = new EuclideanDistance();
		parameters.compactHierarchy = false;

		// Read in the input arguments and assign them to variables:
		for (String argument : args) {
			// Assign input file:
			if (argument.startsWith(FILE_FLAG) && argument.length() > FILE_FLAG.length()) {
				parameters.inputFile = argument.substring(FILE_FLAG.length());
			}
			if (argument.startsWith(CLUSTERNAME_FLAG) && argument.length() > CLUSTERNAME_FLAG.length()) {
				parameters.clusterName = argument.substring(CLUSTERNAME_FLAG.length());
			}
			// Assign minPoints:
			if (argument.startsWith(MIN_PTS_FLAG) && argument.length() > MIN_PTS_FLAG.length()) {
				try {
					parameters.minPoints = Integer.parseInt(argument.substring(MIN_PTS_FLAG.length()));
				} catch (NumberFormatException nfe) {
					System.out.println("Illegal v.distanceFunction");
				}
			} else if (argument.startsWith(K_FLAG) && argument.length() > K_FLAG.length()) {
				try {
					parameters.k = Double.parseDouble(argument.substring(K_FLAG.length()));
				} catch (NumberFormatException nfe) {
					System.out.println("Illegal v.distanceFunction");
				}
			} else if (argument.startsWith(PROCESSING_UNITS) && argument.length() > PROCESSING_UNITS.length()) {
				try {
					parameters.processing_units = Integer.parseInt(argument.substring(PROCESSING_UNITS.length()));
				} catch (NumberFormatException nfe) {
					System.out.println("Illegal value for processing units");
				}
			}
			// Assign minClusterSize:
			else if (argument.startsWith(MIN_CL_SIZE_FLAG) && argument.length() > MIN_CL_SIZE_FLAG.length()) {
				try {
					parameters.minClusterSize = Integer.parseInt(argument.substring(MIN_CL_SIZE_FLAG.length()));
				} catch (NumberFormatException nfe) {
					System.out.println("Illegal value for minClSize.");
				}
			} // Assign compact hierarchy:
			else if (argument.startsWith(COMPACT_FLAG) && argument.length() > COMPACT_FLAG.length()) {
				parameters.compactHierarchy = Boolean.parseBoolean(argument.substring(COMPACT_FLAG.length()));

			} // Assign distance function:
			else if (argument.startsWith(DISTANCE_FUNCTION_FLAG)
					&& argument.length() > DISTANCE_FUNCTION_FLAG.length()) {
				String functionName = argument.substring(DISTANCE_FUNCTION_FLAG.length());

				if (functionName.equals(EUCLIDEAN_DISTANCE)) {
					parameters.distanceFunction = new EuclideanDistance();
				} else if (functionName.equals(COSINE_SIMILARITY)) {
					// parameters.distanceFunction = new CosineSimilarity();
					parameters.distanceFunction = null;
				} else if (functionName.equals(PEARSON_CORRELATION)) {
					// parameters.distanceFunction = new PearsonCorrelation();
					parameters.distanceFunction = null;
				} else if (functionName.equals(MANHATTAN_DISTANCE)) {
					// parameters.distanceFunction = new ManhattanDistance();
					parameters.distanceFunction = null;
				} else if (functionName.equals(SUPREMUM_DISTANCE)) {
					// parameters.distanceFunction = new SupremumDistance();
					parameters.distanceFunction = null;
				} else {
					parameters.distanceFunction = null;
				}
			}
		}
		// Check that each input parameter has been assigned:
		if (parameters.inputFile == null) {
			System.out.println("Missing input file name.");
			printHelpMessageAndExit();
		} else if (parameters.minPoints == null) {
			System.out.println("Missing value for minPts.");
			printHelpMessageAndExit();
		} else if (parameters.k == null) {
			System.out.println("Missing value for k.");
			printHelpMessageAndExit();
		} else if (parameters.minClusterSize == null) {
			System.out.println("Missing value for minClSize");
			printHelpMessageAndExit();
		} else if (parameters.distanceFunction == null) {
			System.out.println("Missing distance function.");
			printHelpMessageAndExit();
		}
		if (parameters.compactHierarchy) {
			parameters.hierarchyFile = "base_compact_hierarchy.csv";
		} else {
			parameters.hierarchyFile = "base_hierarchy.csv";
		}
		parameters.clusterTreeFile = "base_tree.csv";
		parameters.partitionFile = "base_partition.csv";
	// 	parameters.outlierScoreFile = "base_outlier_scores.csv";
		return parameters;
	}

	/**
	 * Prints a help message that explains the usage of HDBSCANStarRunner, and then
	 * exits the program.
	 */
	private static void printHelpMessageAndExit() {
		System.out.println();

		System.out.println("Executes the MR-HDBSCAN* algorithm, which produces a hierarchy, cluster tree, "
				+ "flat partitioning, and outlier scores for an input data set.");
		System.out.println(
				"Usage: ../spark/bin/spark-submit --class main.Main /usr/local/spark/jars/MR-HDBSCANStar.jar file=<input file> minPts=<minPts value> "
						+ "minClSize=<minClSize value> [compact={true,false}] " + "[clusterName=<cluster name>]"
						+ "[processing_units=<processing unit number>]" + "[k=<sampling number>]");
		System.out.println("By default the hierarchy produced is non-compact (full), and euclidean distance is used.");
		System.out.println(
				"Example usage: ../spark/bin/spark-submit --class main.Main /usr/local/spark/jars/MR-HDBSCANStar.jar file=dataset.txt minPts=4 minClSize=4 clusterName=local processing_units=1000 k=0.2");
		System.out.println();
		System.out.println("The input data set file must be a comma-separated value (CSV) file, where each line "
				+ "represents an object, with attributes separated by simple space.");
		System.out.println();
		System.exit(0);
	}

	/**
	 * Simple class for storing input parameters.
	 */
	private static class HDBSCANStarParameters {

		public String inputFile;
		public Integer minPoints;
		public Double k;
		public Integer minClusterSize;
		public boolean compactHierarchy;
		public DistanceCalculator distanceFunction;
		public Integer processing_units;
		public String clusterName;

		public String hierarchyFile;
		public String clusterTreeFile;
		public String partitionFile;
		// public String outlierScoreFile;
		// public String visualizationFile;
	}

}
