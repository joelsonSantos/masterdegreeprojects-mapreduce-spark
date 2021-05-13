package main;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.TreeSet;

import mappers.ComponentToPairMapper;
import mappers.ConstructADJMapper;
import mappers.EdgesMapper;
import mappers.FlatPartitionMapper;
import mappers.AssignClusterLabelsMapper;
import mappers.Change;
import mappers.Refresh;
import mappers.WriteLevelMapper;
import mappers.InversoMapper;
import mappers.RemoveMapper;
import mappers.SizeComponenteMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.Accumulator;

import extra.methods.MinimumSpanningTree;
import extra.methods.Clusters;
import extra.methods.Noise;
import reducers.ConstructADJReducer;
import reducers.MSTReducer;
import reducers.SizeComponenteReducer;
import reducers.UpdateStabilityReducer;
import scala.Tuple2;

import filters.FilterDuplicates;
import filters.FilterEmpates;
import filters.FilterEmptySpaces;
import filters.SolutionFilter;
import flatmappers.CCMRMapper;

public class Main {

	private static final String FILE_FLAG = "file=";
	private static final String CLUSTERNAME_FLAG = "clusterName=";
	private static final String CONSTRAINTS_FLAG = "constraints=";
	private static final String MIN_PTS_FLAG = "minPts=";
	private static final String MIN_CL_SIZE_FLAG = "minClSize=";
	private static final String COMPACT_FLAG = "compact=";

	private static SparkConf conf;
	private static JavaRDD<String> mstFile;

	public static TreeSet<Clusters> globalClustersTree;
	private static LinkedList<Clusters> finalClusters;

	public static boolean newIterationNeeded;

	@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
	public static void main(String[] args) throws Exception {

		// Parse input parameters from program arguments:
		HDBSCANStarParameters parameters = checkInputParameters(args);

		System.out.println("Running CC-MR algorithm on " + parameters.inputFile
				+ " with minPts=" + parameters.minPoints + ", minClSize="
				+ parameters.minClusterSize + ", constraints="
				+ parameters.constraintsFile + ", compact="
				+ parameters.compactHierarchy + ", clusterName="
				+ parameters.clusterName);

		// Tirar isso depois
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Criando um JavaSparkContext
		String inputName = parameters.inputFile;
		if (parameters.inputFile.contains(".")) {
			inputName = parameters.inputFile.substring(0,
					parameters.inputFile.lastIndexOf("."));
		}

		conf = new SparkConf()
				.setAppName("CC-MR algorithm on MST file: " + inputName)
				.setMaster(parameters.clusterName)
				// spark://master:7077
				.set("spark.hadoop.validateOutputSpecs", "false");
		// .set("spark.driver.cores", "6")
		// .set("spark.driver.memory", "6g")
		// .set("spark.executor.cores", "6")
		// .set("spark.executor.memory", "6g");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		mstFile = jsc.textFile(parameters.inputFile);
		mstFile = mstFile.filter(new FilterEmptySpaces())
				.map(new Function<String, String>() {
					private static final long serialVersionUID = 1L;

					public String call(String v1) throws Exception {
						String[] s = v1.split(" ");
						StringBuilder string = new StringBuilder();
						int v = Integer.parseInt(s[0]) + 1;
						int u = Integer.parseInt(s[1]) + 1;
						string.append(v + " " + u + " " + s[2]);

						return string.toString();
					}
				});

		JavaPairRDD<Integer, MinimumSpanningTree> newMST = mstFile
				.filter(new FilterEmptySpaces()).mapToPair(
						new PairFunction<String, Integer, MinimumSpanningTree>() {

							public Tuple2<Integer, MinimumSpanningTree> call(
									String t) throws Exception {
								String[] s = t.split(" ");
								return new Tuple2(1,
										new MinimumSpanningTree(
												Integer.parseInt(s[0]),
												Integer.parseInt(s[1]),
												Double.parseDouble(s[2])));
							}
						});

		int mstSize = (int) mstFile.count();

		JavaRDD<String> inverse = mstFile.map(new InversoMapper());

		JavaRDD<String> simetry = inverse.union(mstFile)
				.map(new Function<String, String>() {
					private static final long serialVersionUID = 1L;

					public String call(String v1) throws Exception {
						return v1;
					}
				});

		JavaPairRDD<Integer, String> mstAdj = simetry
				.filter(new FilterEmptySpaces())
				.mapToPair(new ConstructADJMapper())
				.reduceByKey(new ConstructADJReducer());

		inverse = null;
		simetry = null;

		System.out.println("INIT...");
		long initOverall = System.currentTimeMillis();
		long initClusterTime = System.currentTimeMillis();

		jsc.setCheckpointDir(parameters.inputFile + "/checkpoints");
		newMST.checkpoint();

		// String firstEdgeLevel = mstFile.first();
		Tuple2<Integer, MinimumSpanningTree> first = newMST.first();
		LinkedList<Tuple2<Integer, MinimumSpanningTree>> affectedVerticesList = new LinkedList<Tuple2<Integer, MinimumSpanningTree>>();
		affectedVerticesList.add(first);
		newMST = newMST.filter(new FilterEmpates(first));

		// / remover todos os empates
		while (newMST.first()._2().getWeight() == first._2().getWeight()) {
			affectedVerticesList.add(newMST.first());
			newMST = newMST.filter(new FilterEmpates(newMST.first()));
		}

		int[][] affectedVerticesVect = new int[affectedVerticesList.size()][2];
		for (int i = 0; i < affectedVerticesVect.length; i++) {
			affectedVerticesVect[i][0] = affectedVerticesList.get(i)._2()
					.getVertice1();
			affectedVerticesVect[i][1] = affectedVerticesList.get(i)._2()
					.getVertice2();
		}

		mstAdj = mstAdj.mapToPair(new RemoveMapper(affectedVerticesVect));
		double currentEdge = first._2().getWeight();

		// ////////////////////////////////////////////////
		// Escrever primeiro nível
		Configuration configuration = new Configuration();
		configuration.addResource(
				new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		configuration.addResource(
				new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		FileSystem hdfsFileSystem = FileSystem.get(configuration);

		Path pathLevel = new Path(
				parameters.inputFile + "/level/" + mstSize + "/part-00000");
		BufferedWriter levelWriter;
		levelWriter = new BufferedWriter(
				new OutputStreamWriter(hdfsFileSystem.create(pathLevel, true)));

		int size = ((mstSize + 1) / 2);

		// // init cluster (root)

		globalClustersTree = new TreeSet<Clusters>();
		globalClustersTree.add(new Clusters(1, mstSize, size, 0.0, -1, null, 0,
				new Noise(0, 0, Double.MAX_VALUE, Double.MAX_VALUE),
				new TreeSet<Clusters>()));

		for (int i = 0; i < size - 1; i++) {
			// key : (Ci) - value: vDest , currentLevel, sizeCC, offsetLevel
			levelWriter.write((i + 1) + " " + 1 + " " + currentEdge + " " + size
					+ " " + mstSize + "\n");
		}
		levelWriter.write((size) + " " + 1 + " " + currentEdge + " " + size
				+ " " + mstSize);
		levelWriter.close();

		JavaRDD<String> firstCC = jsc.textFile(
				parameters.inputFile + "/level/" + mstSize + "/part-00000");

		JavaPairRDD<Integer, Clusters> prevLevel = firstCC
				.mapToPair(new WriteLevelMapper());
		// .cache();
		prevLevel.saveAsObjectFile(parameters.inputFile + "/level/" + mstSize);
		prevLevel = null;

		// int i = (int) newMST.count();
		int countCheckPoints1 = 0;
		int countCheckPoints2 = 0;
		int countCheckPoints3 = 0;
		int levels = mstSize;
		int factor = 2;
		int currentEdges = (int) newMST.count();
		while (currentEdges >= 0) {
			JavaRDD<String> component;
			JavaPairRDD<Integer, String> mst = mstAdj;
			Accumulator<Integer> newIter = null;
			int countComp = 0;

			do {
				newIter = jsc.accumulator(0);
				if (countCheckPoints1 == 100) {
					jsc.setCheckpointDir(parameters.inputFile + "/checkpoints");
					// System.out.println("Check on MST");
					mst.checkpoint();
				}

				mst.flatMap(new CCMRMapper(newIter))
						.filter(new FilterEmptySpaces())
						.saveAsTextFile(parameters.inputFile + "/connected/"
								+ countComp);
				mst = null;
				component = jsc.textFile(
						parameters.inputFile + "/connected/" + countComp);

				if (countCheckPoints1 == 100) {
					jsc.setCheckpointDir(parameters.inputFile + "/checkpoints");
					// System.out.println("Check on Component");
					component.checkpoint();
					countCheckPoints1 = 0;
				}

				mst = component.filter(new FilterEmptySpaces())
						.mapToPair(new EdgesMapper())
						.reduceByKey(new MSTReducer());
				countCheckPoints1++;
				countComp++;
			} while (newIter.value() > 0);

			JavaPairRDD<Integer, String> sizeCC = component
					.filter(new FilterEmptySpaces())
					.mapToPair(new SizeComponenteMapper(affectedVerticesVect))
					.reduceByKey(new SizeComponenteReducer());

			// sizeCC.saveAsTextFile(parameters.inputFile + "/size/" + (levels -
			// 1));

			if (countCheckPoints2 == 100) {
				jsc.setCheckpointDir(parameters.inputFile + "/checkpoints");
				newMST.checkpoint();
				mstAdj.checkpoint();
				countCheckPoints2 = 0;
			}

			if (currentEdges > 0) {
				first = newMST.first();
				currentEdges--;
			}

			double oldEdge = currentEdge;
			currentEdge = first._2().getWeight();

			affectedVerticesList = new LinkedList<Tuple2<Integer, MinimumSpanningTree>>();
			affectedVerticesList.add(first);
			newMST = newMST.filter(new FilterEmpates(first));

			Tuple2<Integer, MinimumSpanningTree> second = null;

			if (currentEdges > 0) {
				second = newMST.first();
			} else {
				System.out.println("Empty");
			}
			// / remover todos os empates
			if (second != null) {
				while (currentEdges > 0 && (second._2().getWeight() == first
						._2().getWeight())) {
					affectedVerticesList.add(second);
					newMST = newMST.filter(new FilterEmpates(second));
					currentEdges--;
					if (currentEdges > 0) {
						second = newMST.first();
					}

					if (countCheckPoints3 == 100) {
						jsc.setCheckpointDir(
								parameters.inputFile + "/checkpoints");
						newMST.checkpoint();
						mstAdj.checkpoint();
						countCheckPoints3 = 0;

					}
					countCheckPoints3++;
				}
			}

			affectedVerticesVect = new int[affectedVerticesList.size()][2];
			for (int j = 0; j < affectedVerticesVect.length; j++) {
				affectedVerticesVect[j][0] = affectedVerticesList.get(j)._2()
						.getVertice1();
				affectedVerticesVect[j][1] = affectedVerticesList.get(j)._2()
						.getVertice2();
			}

			mstAdj = mstAdj.mapToPair(new RemoveMapper(affectedVerticesVect));

			JavaPairRDD<Integer, String> currentLevel = component
					.filter(new FilterEmptySpaces())
					.filter(new FilterDuplicates())
					.mapToPair(new ComponentToPairMapper()).join(sizeCC)
					.mapToPair(new Refresh(currentEdge));

			component = null;

			LinkedList<Clusters> listc = new LinkedList<Clusters>(
					globalClustersTree);

			Clusters[] vectorClusters = new Clusters[listc.size()];

			for (int g = 0; g < vectorClusters.length; g++) {
				vectorClusters[g] = listc.get(g);
			}

			JavaRDD<Tuple2<Integer, Clusters>> prev = jsc
					.objectFile(parameters.inputFile + "/level/" + levels);
			JavaPairRDD<Integer, Clusters> newPrev = JavaPairRDD
					.fromJavaRDD(prev).mapToPair(
							new PairFunction<Tuple2<Integer, Clusters>, Integer, Clusters>() {
								public Tuple2<Integer, Clusters> call(
										Tuple2<Integer, Clusters> t)
										throws Exception {
									return new Tuple2(t._1(), t._2());
								}

							})
					.join(currentLevel)
					.mapToPair(new AssignClusterLabelsMapper(
							parameters.minClusterSize, (levels - 1), oldEdge,
							factor, vectorClusters));

			newPrev.saveAsObjectFile(
					parameters.inputFile + "/level/" + (levels - 1));

			JavaPairRDD<Integer, Clusters> stability = newPrev
					.mapToPair(new Change());

			stability = stability
					.reduceByKey(new UpdateStabilityReducer(currentEdge));

			LinkedList<Tuple2<Integer, Clusters>> listS = new LinkedList<Tuple2<Integer, Clusters>>();
			listS.addAll(stability.collect());

			// add os clusters não existentes em globalClustersTree
			for (int j = 0; j < listS.size(); j++) {
				boolean exists = false;
				for (Clusters cls : globalClustersTree) {
					if (cls.getId() == listS.get(j)._2().getId()) {
						exists = true;
					}
				}
				if (!exists) {
					globalClustersTree.add(listS.get(j)._2());
				}
			}

			// cria arvore de grupos - centralizado
			TreeSet<Clusters> remove = new TreeSet<Clusters>(
					globalClustersTree);
			for (Clusters clust : globalClustersTree) {
				for (int j = 0; j < listS.size(); j++) {
					if (listS.get(j)._2().getChildren() != null) {
						if (clust.getId() == listS.get(j)._2().getId()) {
							Clusters obj = clust;
							remove.remove(clust);
							obj.setStability(obj.getStability()
									+ listS.get(j)._2().getStability());
							obj.getChildren()
									.addAll(listS.get(j)._2().getChildren());

							obj.setDeathLevel(Math.min(obj.getDeathLevel(),
									listS.get(j)._2().getDeathLevel()));
							remove.add(obj);
						}
					}
				}
			}
			countCheckPoints2++;
			globalClustersTree = remove;
			System.out.println(" Stage: " + (levels - 1));
			// i -= affectedVerticesList.size();
			levels--;
			factor++;
		}

		long endClusterTime = System.currentTimeMillis() - initClusterTime;

		Path pathCluster = new Path(
				parameters.inputFile + "/ClusterTime" + "/part-00000");
		BufferedWriter clusterWriter;
		clusterWriter = new BufferedWriter(new OutputStreamWriter(
				hdfsFileSystem.create(pathCluster, true)));
		clusterWriter.write("" + endClusterTime);
		clusterWriter.close();

		finalClusters = new LinkedList<Clusters>(globalClustersTree);

		System.out.println("Filhos antes: ");
		for (int t = 0; t < finalClusters.size(); t++) {
			System.out.println(finalClusters.get(t).getChildren());
		}

		// update children
		for (Clusters cls1 : finalClusters) {
			for (Clusters cls2 : finalClusters) {
				if (!cls1.getChildren().isEmpty()) {
					if (cls1.getChildren().contains(cls2)) {
						cls1.getChildren().remove(cls2);
						cls1.getChildren().add(cls2);
					}
				}
			}
		}

		System.out.println("Filhos Depois: ");
		for (int t = 0; t < finalClusters.size(); t++) {
			System.out.println(finalClusters.get(t).getChildren());
		}

		////////////////// FLAT PARTITION //////////////////////////

		for (int t = 0; t < finalClusters.size(); t++) {
			System.out
					.println("pai: " + finalClusters.get(t).getParentCluster());
			System.out.println("filho: " + finalClusters.get(t));
		}

		long timeFlat = System.currentTimeMillis();
		Collections.sort(finalClusters, new Clusters());
		LinkedList<Clusters> sol = findSolution();

		for (int position = 0; position < sol.size(); position++) {
			if (sol.get(position) != null) {
				// filter ////
				JavaRDD<Tuple2<Integer, Clusters>> flatFile = jsc
						.objectFile(parameters.inputFile + "/level/"
								+ sol.get(position).getOffsetLevel());
				JavaPairRDD<Integer, Clusters> flatPartition = JavaPairRDD
						.fromJavaRDD(flatFile)
						.mapToPair(new FlatPartitionMapper())
						.filter(new SolutionFilter(sol.get(position).getId()));
				flatPartition.saveAsTextFile(
						parameters.inputFile + "/flatPartition/" + position);
			}
		}

		long endFlat = System.currentTimeMillis() - timeFlat;

		long endOverall = System.currentTimeMillis() - initOverall;
		System.out.println("end: OverallTime... " + endOverall / 1000);

		Path pathOverall = new Path(
				parameters.inputFile + "/OverallTime" + "/part-00000");
		BufferedWriter overallWriter;
		overallWriter = new BufferedWriter(new OutputStreamWriter(
				hdfsFileSystem.create(pathOverall, true)));
		overallWriter.write("" + endOverall);
		overallWriter.close();

		Path pathFlat = new Path(
				parameters.inputFile + "/FlatTime" + "/part-00000");
		BufferedWriter flatWriter;
		flatWriter = new BufferedWriter(
				new OutputStreamWriter(hdfsFileSystem.create(pathFlat, true)));
		flatWriter.write("" + endFlat);
		flatWriter.close();
	}

	public static LinkedList<Clusters> findSolution() {

		for (int position = 0; position < finalClusters.size(); position++) {
			if (!finalClusters.get(position).getChildren().isEmpty()) {
				double stability = 0.0;
				LinkedList<Clusters> children = new LinkedList<Clusters>(
						finalClusters.get(position).getChildren());
				for (int p = 0; p < children.size(); p++) {
					stability += children.get(p).getStability();
				}
				if (finalClusters.get(position).getStability() < stability) {
					finalClusters.get(position).setStability(stability);
					finalClusters.get(position).setSolution(false);
				} else { // busca em profundidade na sub-árvore do
							// clusters(position)
					for (int a = 0; a < finalClusters.size(); a++) {
						finalClusters.get(a).setVisited(false);
					}
					dfs(finalClusters.get(position));
				}
			}
		}
		LinkedList<Clusters> solution = new LinkedList<Clusters>();
		for (int i = 0; i < finalClusters.size(); i++) {
			if (finalClusters.get(i).isSolution()) {
				solution.add(finalClusters.get(i));
			}
		}

		return solution;
	}

	public static void dfs(Clusters clusters) {
		LinkedList<Clusters> adj = new LinkedList<Clusters>(
				clusters.getChildren());
		clusters.setVisited(true);
		for (int i = 0; i < adj.size(); i++) {
			if (!adj.get(i).isVisited()) {
				for (int j = 0; j < finalClusters.size(); j++) {
					if (adj.get(i).getId() == finalClusters.get(j).getId()) {
						finalClusters.get(j).setSolution(false);
						dfs(finalClusters.get(j));
					}
				}
			}
		}
	}

	private static HDBSCANStarParameters checkInputParameters(String[] args) {
		HDBSCANStarParameters parameters = new HDBSCANStarParameters();
		parameters.compactHierarchy = false;

		// Read in the input arguments and assign them to variables:
		for (String argument : args) {

			// Assign input file:
			if (argument.startsWith(FILE_FLAG)
					&& argument.length() > FILE_FLAG.length()) {
				parameters.inputFile = argument.substring(FILE_FLAG.length());
			}

			if (argument.startsWith(CLUSTERNAME_FLAG)
					&& argument.length() > CLUSTERNAME_FLAG.length()) {
				parameters.clusterName = argument
						.substring(CLUSTERNAME_FLAG.length());
			}

			// Assign constraints file:
			if (argument.startsWith(CONSTRAINTS_FLAG)
					&& argument.length() > CONSTRAINTS_FLAG.length()) {
				parameters.constraintsFile = argument
						.substring(CONSTRAINTS_FLAG.length());
			} // Assign minPoints:
			else if (argument.startsWith(MIN_PTS_FLAG)
					&& argument.length() > MIN_PTS_FLAG.length()) {
				try {
					parameters.minPoints = Integer.parseInt(
							argument.substring(MIN_PTS_FLAG.length()));
				} catch (NumberFormatException nfe) {
					System.out.println("Illegal v.distanceFunction");
				}
			}

			// Assign minClusterSize:
			else if (argument.startsWith(MIN_CL_SIZE_FLAG)
					&& argument.length() > MIN_CL_SIZE_FLAG.length()) {
				try {
					parameters.minClusterSize = Integer.parseInt(
							argument.substring(MIN_CL_SIZE_FLAG.length()));
				} catch (NumberFormatException nfe) {
					System.out.println("Illegal value for minClSize.");
				}
			} // Assign compact hierarchy:
			else if (argument.startsWith(COMPACT_FLAG)
					&& argument.length() > COMPACT_FLAG.length()) {
				parameters.compactHierarchy = Boolean.parseBoolean(
						argument.substring(COMPACT_FLAG.length()));

			} // Assign distance function:

		}

		// Check that each input parameter has been assigned:
		if (parameters.inputFile == null) {
			System.out.println("Missing input file name.");
			printHelpMessageAndExit();
		} else if (parameters.minPoints == null) {
			System.out.println("Missing value for minPts.");
			printHelpMessageAndExit();
		} else if (parameters.minClusterSize == null) {
			System.out.println("Missing value for minClSize");
			printHelpMessageAndExit();
		}

		// Generate names for output files:
		String inputName = parameters.inputFile;
		if (parameters.inputFile.contains(".")) {
			inputName = parameters.inputFile.substring(0,
					parameters.inputFile.lastIndexOf("."));
		}

		if (parameters.compactHierarchy) {
			parameters.hierarchyFile = inputName
					+ "/resultados/hdbscan/base_compact_hierarchy.csv";
		} else {
			parameters.hierarchyFile = inputName
					+ "/resultados/hdbscan/base_hierarchy.csv";
		}
		parameters.clusterTreeFile = inputName
				+ "/resultados/hdbscan/base_tree.csv";
		parameters.partitionFile = inputName
				+ "/resultados/hdbscan/base_partition.csv";
		parameters.outlierScoreFile = inputName
				+ "/resultados/hdbscan/base_outlier_scores.csv";
		parameters.visualizationFile = inputName
				+ "/resultados/hdbscan/base_visualization.vis";

		return parameters;
	}

	/**
	 * Prints a help message that explains the usage of HDBSCANStarRunner, and
	 * then exits the program.
	 */
	private static void printHelpMessageAndExit() {
		System.out.println();

		System.out.println(
				"Executes the CC-MR algorithm, which produces a hierarchy, cluster tree, "
						+ "flat partitioning, and outlier scores for an input data set.");
		System.out.println(
				"Usage: java -jar cc-mr-algorithm.jar file=<input file> minPts=<minPts value> "
						+ "minClSize=<minClSize value> [constraints=<constraints file>] [compact={true,false}] ");

		System.exit(0);
	}

	/**
	 * Simple class for storing input parameters.
	 */
	private static class HDBSCANStarParameters {

		public String inputFile;
		public String constraintsFile;
		public Integer minPoints;
		public Integer minClusterSize;
		public boolean compactHierarchy;
		public String clusterName;

		public String hierarchyFile;
		public String clusterTreeFile;
		public String partitionFile;
		public String outlierScoreFile;
		public String visualizationFile;
	}

}
