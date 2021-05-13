package extra.methods;

import java.io.Serializable;
import java.util.Comparator;
import java.util.TreeSet;

public class Clusters implements Serializable, Comparable<Clusters>,
		Comparator<Clusters> {

	private static final long serialVersionUID = 1L;
//	String hierarchyLevel;
	private int id;
//	private int idObject;
	private int offsetLevel;
	private int numPoints;
	private double stability;
	private double birthLevel;
	private double deathLevel;
	private boolean isNoise;
	private boolean solution;
	private boolean visited;
	private boolean newCluster;
	private int changed;
	private Clusters parentCluster;
	private Noise noise;
	private TreeSet<Clusters> children;

	public Clusters() {

	}

	// valid clusters
	public Clusters(int id, int offsetLevel, int numPoints,
			double birthLevel, double stability, Clusters parentCluster,
			int changed, Noise noise, TreeSet<Clusters> children) {
		//this.setHierarchyLevel(hierarchy);
		this.setId(id);
		this.setNumPoints(numPoints);
		this.setBirthLevel(birthLevel);
		this.setStability(stability);
		this.setIsNoise(false);
		this.setOffsetLevel(offsetLevel);
		this.setParentCluster(parentCluster);
		this.setChanged(changed);
		this.setNoise(noise);
		this.setChildren(children);
		this.setSolution(true);
		this.setVisited(false);
	}

	// noise clusters
	public Clusters(boolean isNoise, int changed, Noise noise, Clusters parent,
			TreeSet<Clusters> clusters) {
		this.setIsNoise(isNoise);
		this.setChanged(changed);
		this.setNoise(noise);
		this.setParentCluster(parent);
		this.setChildren(clusters);
		this.setVisited(false);
	}

	public void calculateStability(int numPoints, double level, double birthLevel) {
		if (numPoints > 0) {
			stability = numPoints * ((1 / level) - (1 / birthLevel));
		}
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getOffsetLevel() {
		return offsetLevel;
	}

	public void setOffsetLevel(int offsetLevel) {
		this.offsetLevel = offsetLevel;
	}

	public int getNumPoints() {
		return numPoints;
	}

	public void setNumPoints(int numPoints) {
		this.numPoints = numPoints;
	}

	public double getStability() {
		return stability;
	}

	public void setStability(double stability) {
		this.stability = stability;
	}

	public boolean isNoise() {
		return isNoise;
	}

	public void setIsNoise(boolean isNoise) {
		this.isNoise = isNoise;
	}

	public double getDeathLevel() {
		return deathLevel;
	}

	public void setDeathLevel(double deathLevel) {
		this.deathLevel = deathLevel;
	}

	public double getBirthLevel() {
		return birthLevel;
	}

	public void setBirthLevel(double birthLevel) {
		this.birthLevel = birthLevel;
	}

	public Clusters getParentCluster() {
		return parentCluster;
	}

	public void setParentCluster(Clusters parentCluster) {
		this.parentCluster = parentCluster;
	}

	public int isChanged() {
		return changed;
	}

	public void setChanged(int changed) {
		this.changed = changed;
	}

	public Noise getNoise() {
		return noise;
	}

	public void setNoise(Noise noise) {
		this.noise = noise;
	}

	public TreeSet<Clusters> getChildren() {
		return children;
	}

	public void setChildren(TreeSet<Clusters> children) {
		this.children = children;
	}

	public int compareTo(Clusters cluster) {
		return this.getId() - cluster.getId();
	}

	public boolean isSolution() {
		return solution;
	}

	public void setSolution(boolean solution) {
		this.solution = solution;
	}

	public int compare(Clusters arg0, Clusters arg1) {
		if (arg0.getOffsetLevel() > arg1.getOffsetLevel()) {
			return 1;
		} else {
			return -1;
		}
	}

	public boolean isVisited() {
		return visited;
	}

	public void setVisited(boolean visited) {
		this.visited = visited;
	}

	public boolean isNewCluster() {
		return newCluster;
	}

	public void setNewCluster(boolean newCluster) {
		this.newCluster = newCluster;
	}

	@Override
	public String toString() {
		return "Clusters [solution=" + solution + " stability=" + stability + " id=" + id+  "]";
	}
}
