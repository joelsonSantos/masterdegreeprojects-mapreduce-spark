package reducers;

import org.apache.spark.api.java.function.Function2;

import extra.methods.Clusters;

public class UpdateStabilityAndOutlierScoreReducer
		implements Function2<Clusters, Clusters, Clusters> {

	private static final long serialVersionUID = 1L;
	private double level;

	public UpdateStabilityAndOutlierScoreReducer(double level) {
		this.level = level;
	}

	public Clusters call(Clusters v1, Clusters v2) throws Exception {
		if (v1.getParentCluster() != null) {
			v1.setChanged(v1.isChanged() + v2.isChanged());
			if ((!v1.isNoise() && !v2.isNoise()) && v1.getId() != 1) {
				v1.calculateStability(v1.isChanged(), this.level, v1.getBirthLevel());
				v1.getParentCluster().getChildren().addAll(v2.getParentCluster().getChildren());
			}
		}
		v1.getNoise().setfMaxXi(Math.min(v1.getNoise().getfMaxXi(), v2.getNoise().getfMaxXi()));
		//System.out.println(" Cluster: " + v1.getId() + " Stability: " + v1.getStability());
		return v1;
	}
}
