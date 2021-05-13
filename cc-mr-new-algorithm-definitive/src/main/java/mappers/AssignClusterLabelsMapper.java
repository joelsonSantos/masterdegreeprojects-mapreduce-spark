package mappers;

import java.util.TreeSet;

import org.apache.spark.api.java.function.PairFunction;

import extra.methods.Clusters;
import extra.methods.Noise;
import scala.Tuple2;

public class AssignClusterLabelsMapper implements
		PairFunction<Tuple2<Integer, Tuple2<Clusters, String>>, Integer, Clusters> {

	private static final long serialVersionUID = 1L;
	private int minClSize;
	private int offsetLevel;
	private int factor;
	private double oldEdge;
	private Clusters[] vector;

	public AssignClusterLabelsMapper(int minClSize, int offsetLevel,
			double oldEdge, int factor, Clusters[] vector) {
		this.minClSize = minClSize;
		this.offsetLevel = offsetLevel;
		this.oldEdge = oldEdge;
		this.factor = factor;
		this.vector = vector;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple2<Integer, Clusters> call(
			Tuple2<Integer, Tuple2<Clusters, String>> t)
			throws Exception {

		Clusters currentLevel = null;

		String[] currentTuple = t._2()._2().split(" ");
		Clusters previousLevel = t._2()._1();
		int currentSize = Integer.parseInt(currentTuple[2]);
		boolean has = Boolean.parseBoolean(currentTuple[3]);
		int component = Integer.parseInt(currentTuple[0]);
		double currentEdge = Double.parseDouble(currentTuple[1]);

		if (has) {
			// new cluster
			// if ((currentSize >= this.minClSize) &&
			// ((previousLevel.getNumPoints() - currentSize) >= this.minClSize))
			// {
			if (currentSize >= this.minClSize) {
				int label = generateLabel(component);
				// retornar currentEdge = birthLevel, neste ponto.

				TreeSet<Clusters> children = new TreeSet<Clusters>();
				currentLevel = new Clusters();

				// currentLevel.setHierarchyLevel(null);
				currentLevel.setId(label);
				currentLevel.setOffsetLevel(this.offsetLevel);
				currentLevel.setNumPoints(currentSize);
				currentLevel.setBirthLevel(this.oldEdge);
				currentLevel.setStability(0.0);
				currentLevel.setIsNoise(false);
				currentLevel.setSolution(true);
				currentLevel.setNewCluster(true);
				currentLevel.setNoise(
						new Noise(t._1(), 0, currentEdge, currentEdge));
				currentLevel.setChildren(new TreeSet<Clusters>());
				children.add(currentLevel);
				previousLevel.setChildren(children);
				previousLevel.setDeathLevel(this.oldEdge);
				previousLevel.setChanged(1);
				currentLevel.setParentCluster(previousLevel);
			}
			// propaga o rotulo
			// else if ((((previousLevel.getNumPoints()
			// - currentSize) < this.minClSize)
			// || (previousLevel.getNumPoints() == currentSize))
			// && (currentSize >= this.minClSize)) { // propagate old label
			// // and
			// // b
			//
			// // previousLevel.setHierarchyLevel(string.toString());
			// previousLevel.setNumPoints(currentSize);
			// currentLevel = previousLevel;
			// currentLevel.setNewCluster(false);
			// noise label
			else if (currentSize < this.minClSize) {
				// string.append(t._1() + " " + "0" + " " + currentEdge + " "
				// + currentSize + " " + this.offsetLevel);
				currentLevel = new Clusters();
				if (!previousLevel.isNoise()) {
					currentLevel.setIsNoise(true);
					currentLevel.setNoise(previousLevel.getNoise());
					currentLevel.getNoise().setfXi(this.oldEdge);
					currentLevel.setChanged(1);
					currentLevel.getNoise().setId(previousLevel.getId());
					currentLevel.setDeathLevel(currentEdge);
					currentLevel.getNoise().setfMaxXi(currentEdge);
					
					//previousLevel.getNoise().setfXi(this.oldEdge);
					//previousLevel.setIsNoise(true);
					//previousLevel.setChanged(1);
					//previousLevel.getNoise().setId(previousLevel.getId());
					//previousLevel.setDeathLevel(currentEdge);
					//previousLevel.getNoise().setfMaxXi(currentEdge);
					for (Clusters clusters : this.vector) {
						if (clusters.getId() == previousLevel.getId()) {
							propagate(currentLevel,
									currentLevel.getNoise().getfMaxXi());
						}
					}
					/////////////////
				} else {
					currentLevel.setChanged(0);
					//previousLevel.setChanged(0);
				}

				for (int i = 0; i < this.vector.length; i++) {
					if (this.vector[i].getId() == currentLevel.getNoise()
							.getId()) {
						// calculate outlierness score
						currentLevel.getNoise().calculateScore(
								this.vector[i].getNoise().getfMaxXi(),
								currentLevel.getNoise().getfXi());
					}
				}
				//currentLevel = previousLevel;
				currentLevel.setNewCluster(false);
			}
		} else {
			// propaga o rotulo
			previousLevel.setNumPoints(currentSize);
			currentLevel = previousLevel;
			currentLevel.setNewCluster(false);
		}
		return new Tuple2(t._1(), currentLevel);
	}

	private int generateLabel(int currentSize) {
		return currentSize * this.factor;
	}

	private int findClusters(Clusters previousLevel) {
		int position = 0;
		for (int i = 0; i < this.vector.length; i++) {
			if (this.vector[i].getId() == previousLevel.getId()) {
				position = i;
			}
		}
		return position;
	}

	public void propagate(Clusters cluster, double currentEdge) {
		Clusters parent = null;
		if (cluster.getId() != 1) {
			parent = findParentOnClusterTree(cluster);
			// System.out.println(" Child: " + cluster.getId() + " Parent: " +
			// cluster.getParentCluster().getId());
			// if (parent.getId() != 1) {
			propagate(parent, cluster.getNoise().getfMaxXi());
			int position = findClusters(parent);
			this.vector[position].getNoise().setfMaxXi(currentEdge);
			// mainClusters.add(parent);
			// }
		}
	}

	private Clusters findParentOnClusterTree(Clusters cluster) {
		// System.out.println("Cluster analisado: "+ cluster.getId());
		for (int i = 0; i < this.vector.length; i++) {
			if (cluster.getParentCluster().getId() == this.vector[i].getId()) {
				return this.vector[i];
			}
		}
		return null;
	}
}
