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

public class Query5 {
	public static void main(String[] args) throws Exception {

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, "./bsbm/dataset.ttl");

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?product", null, "?productLabel"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product71", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
			.map(new T2SM_MF(null, null, "?prodFeature"));

		DataSet<SolutionMapping> sm3 = sm1.cross(sm2)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm4 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
			.map(new T2SM_MF("?product", null, "?prodFeature"));

		DataSet<SolutionMapping> sm5 = sm3.join(sm4)
			.where(new SM_JKS(new String[]{"?product", "?prodFeature"}))
			.equalTo(new SM_JKS(new String[]{"?product", "?prodFeature"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm6 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product71", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new T2SM_MF(null, null, "?origProperty1"));

		DataSet<SolutionMapping> sm7 = sm5.cross(sm6)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm8 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new T2SM_MF("?product", null, "?simProperty1"));

		DataSet<SolutionMapping> sm9 = sm7.join(sm8)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm10 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product71", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2", null))
			.map(new T2SM_MF(null, null, "?origProperty2"));

		DataSet<SolutionMapping> sm11 = sm9.cross(sm10)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm12 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2", null))
			.map(new T2SM_MF("?product", null, "?simProperty2"));

		DataSet<SolutionMapping> sm13 = sm11.join(sm12)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm14 = sm13
			.filter(new SM2SM_FF("(!= <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product71> ?product)"));

		DataSet<SolutionMapping> sm15 = sm14
			.filter(new SM2SM_FF("(&& (< ?simProperty1 1500) (> ?simProperty1 120))"));

		DataSet<SolutionMapping> sm16 = sm15
			.filter(new SM2SM_FF("(&& (< ?simProperty2 1500) (> ?simProperty2 120))"));

		DataSet<SolutionMapping> sm17 = sm16
			.map(new SM2SM_PF(new String[]{"?product", "?productLabel"}));

		DataSet<SolutionMapping> sm18 = sm17
			.distinct(new SM_DKS());

		DataSet<SolutionMapping> sm19;
		Node node = sm18.collect().get(0).getValue("?productLabel");
		if(node.isLiteral()) {
			if(node.getLiteralValue().getClass().equals(BigDecimal.class) || node.getLiteralValue().getClass().equals(Double.class)){
				sm19 = sm18
					.sortPartition(new SM_OKS_Double("?productLabel"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(BigInteger.class) || node.getLiteralValue().getClass().equals(Integer.class)) {
				sm19 = sm18
					.sortPartition(new SM_OKS_Integer("?productLabel"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(Float.class)) {
				sm19 = sm18
					.sortPartition(new SM_OKS_Float("?productLabel"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(Long.class)){
				sm19 = sm18
					.sortPartition(new SM_OKS_Long("?productLabel"), Order.ASCENDING)
					.setParallelism(1);
			} else {
				sm19 = sm18
					.sortPartition(new SM_OKS_String("?productLabel"), Order.ASCENDING)
					.setParallelism(1);
			}
		} else {
				sm19 = sm18
					.sortPartition(new SM_OKS_String("?productLabel"), Order.ASCENDING)
					.setParallelism(1);
		}

		DataSet<SolutionMapping> sm20 = sm19
			.first(5);

		//************ Sink  ************
		sm20.writeAsText("./Query5-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		sm20.print();
	}
}