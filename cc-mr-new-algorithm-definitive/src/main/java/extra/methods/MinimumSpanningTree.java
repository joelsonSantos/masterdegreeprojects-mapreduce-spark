package extra.methods;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

public class MinimumSpanningTree implements Writable, Serializable{

	private static final long serialVersionUID = 1L;
	private int node;
	private int vertice1;
	private int vertice2;
	private int fakeId1;
	private int fakeId2;
	private double self;
	private double weight;
	private double totalWeight;
	private MinimumSpanningTree mst;
	private LinkedList<MinimumSpanningTree> listMst;

	public MinimumSpanningTree() {
	}

	public MinimumSpanningTree(int vertice1, int vertice2, double weight, double totalWeight, double self) {
		this.vertice1 = vertice1;
		this.vertice2 = vertice2;
		this.weight = weight;
		this.totalWeight = totalWeight;
		this.setSelf(self);
	}
	
	public MinimumSpanningTree(int vertice1, int vertice2, double weight, double totalWeight) {
		this.vertice1 = vertice1;
		this.vertice2 = vertice2;
		this.weight = weight;
		this.totalWeight = totalWeight;
	}

	public MinimumSpanningTree(MinimumSpanningTree mst) {
		this.setMst(mst);
	}
	
	public MinimumSpanningTree(LinkedList<MinimumSpanningTree> list){
		this.listMst = list;
	}

	public MinimumSpanningTree(int vertice1, int vertice2, double weight) {
		this.setVertice1(vertice1);
		this.setVertice2(vertice2);
		this.setWeight(weight);
	}

	public MinimumSpanningTree(int vertice1, int vertice2, double distance, int id1, int id2, int node) {
	    this.setVertice1(vertice1);
	    this.setVertice2(vertice2);
	    this.setWeight(distance);
	    this.setFakeId1(id1);
	    this.setFakeId2(id2);
	}

	public int getVertice1() {
		return vertice1;
	}

	public void setVertice1(int vertice1) {
		this.vertice1 = vertice1;
	}

	public int getVertice2() {
		return vertice2;
	}

	public void setVertice2(int vertice2) {
		this.vertice2 = vertice2;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public double getSelf() {
		return self;
	}

	public void setSelf(double self) {
		this.self = self;
	}

	public double getTotalWeight() {
		return totalWeight;
	}

	public void setTotalWeight(double totalWeight) {
		this.totalWeight = totalWeight;
	}

	public void readFields(DataInput in) throws IOException {
		this.vertice1 = in.readInt();
		this.vertice2 = in.readInt();
        this.self = in.readDouble();
        this.weight = in.readDouble();
        this.totalWeight = in.readDouble();
	}

	// Writable methods
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vertice1);
		out.writeInt(this.vertice2);
		out.writeDouble(this.self);
		out.writeDouble(this.weight);
		out.writeDouble(this.totalWeight);
	}
	
	public LinkedList<MinimumSpanningTree> getListMst() {
		return listMst;
	}

	public void setListMst(LinkedList<MinimumSpanningTree> listMst) {
		this.listMst = listMst;
	}

	public MinimumSpanningTree getMst() {
		return mst;
	}

	public void setMst(MinimumSpanningTree mst) {
		this.mst = mst;
	}

	@Override
	public String toString() {
		return String.format(vertice1 + " " + vertice2 + " " + weight + "\n");
	}

	public int getFakeId1() {
		return fakeId1;
	}

	public void setFakeId1(int fakeId1) {
		this.fakeId1 = fakeId1;
	}

	public int getFakeId2() {
		return fakeId2;
	}

	public void setFakeId2(int fakeId2) {
		this.fakeId2 = fakeId2;
	}

	public int getNode() {
		return node;
	}

	public void setNode(int node) {
		this.node = node;
	}
}
