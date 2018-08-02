import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.univalle.rdf.runner.functions.*;
import org.univalle.rdf.runner.LoadTransformTriples;
import org.univalle.rdf.runner.functions.order.*;
import java.math.*;

public class Query11 {
	public static void main(String[] args) throws Exception {

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, "./bsbm/dataset.ttl");

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature405", null, null))
			.map(new T2SM_MF(null, "?property", "?hasValue"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF(null, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature405"))
			.map(new T2SM_MF("?isValueOf", "?property", null));

		DataSet<SolutionMapping> sm3 = sm1.union(sm2);

		DataSet<SolutionMapping> sm4 = sm3
			.map(new SM2SM_PF(new String[]{"?property", "?hasValue", "?isValueOf"}));

		//************ Sink  ************
		sm4.writeAsText("./Query11-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		sm4.print();
	}
}