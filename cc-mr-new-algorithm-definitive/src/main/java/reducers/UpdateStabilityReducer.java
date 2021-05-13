package reducers;

import org.apache.spark.api.java.function.Function2;

import extra.methods.Clusters;

public class UpdateStabilityReducer
		implements Function2<Clusters, Clusters, Clusters> {

	private static final long serialVersionUID = 1L;
	private double currentEdge;
	private int count = 0;

	public UpdateStabilityReducer(double current) {
		this.currentEdge = current;
	}

	public Clusters call(Clusters v1, Clusters v2) throws Exception {
		if (v1.getId() != 1) {
			count += v1.isChanged() + v2.isChanged();
			v1.calculateStability(count, this.currentEdge, v1.getBirthLevel());
			v1.getNoise().setfMaxXi(Math.min(v1.getNoise().getfMaxXi(),
					v2.getNoise().getfMaxXi()));
		}
		if ((v1.getChildren() != null) && (v2.getChildren() != null)){
			v1.getChildren().addAll(v2.getChildren());
		}
		return v1;
	}
}
