package extra.methods;

import java.io.Serializable;

public class Noise implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private int idPoint;
	private int id;
	private double fXi;
	private double fMaxXi;
	private double score;

	public Noise() {

	}

	public Noise(int idPoint, int id, double maxFxi, double fXi) {
		this.setId(id);
		this.setIdPoint(idPoint);
		this.setfXi(fXi);
		this.setfMaxXi(maxFxi);
		this.calculateScore(maxFxi, fXi);
	}

	public void calculateScore(double fxiMax, double fxi) {
		   if(fxi != 0){
			   score = 1 - (fxiMax / fxi);
		   }
	}

	public int getIdPoint() {
		return idPoint;
	}

	public void setIdPoint(int idPoint) {
		this.idPoint = idPoint;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public double getfXi() {
		return fXi;
	}

	public void setfXi(double fXi) {
		this.fXi = fXi;
	}

	public double getfMaxXi() {
		return fMaxXi;
	}

	public void setfMaxXi(double fMaxXi) {
		this.fMaxXi = fMaxXi;
	}

	@Override
	public String toString() {
		return "Noise [idPoint=" + idPoint + ", id=" + id + ", fXi=" + fXi
				+ ", fMaxXi=" + fMaxXi + ", score=" + score + "]";
	}
}
