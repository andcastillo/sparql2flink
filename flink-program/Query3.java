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

public class Query3 {
	public static void main(String[] args) throws Exception {

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, "./bsbm/dataset.ttl");

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?product", null, "?label"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType7"))
			.map(new T2SM_MF("?product", null, null));

		DataSet<SolutionMapping> sm3 = sm1.join(sm2)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm4 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature263"))
			.map(new T2SM_MF("?product", null, null));

		DataSet<SolutionMapping> sm5 = sm3.join(sm4)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm6 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new T2SM_MF("?product", null, "?p1"));

		DataSet<SolutionMapping> sm7 = sm5.join(sm6)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm8 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric3", null))
			.map(new T2SM_MF("?product", null, "?p3"));

		DataSet<SolutionMapping> sm9 = sm7.join(sm8)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm10 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature237"))
			.map(new T2SM_MF("?product", null, null));

		DataSet<SolutionMapping> sm11 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?product", null, "?testVar"));

		DataSet<SolutionMapping> sm12 = sm10.join(sm11)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm13 = sm9.leftOuterJoin(sm12)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_LOJF());

		DataSet<SolutionMapping> sm14 = sm13
			.filter(new SM2SM_FF("(> ?p1 1)"));

		DataSet<SolutionMapping> sm15 = sm14
			.filter(new SM2SM_FF("(< ?p3 1500)"));

		DataSet<SolutionMapping> sm16 = sm15
			.map(new SM2SM_PF(new String[]{"?product", "?label"}));

		DataSet<SolutionMapping> sm17 = sm16
			.distinct(new SM_DKS());

		DataSet<SolutionMapping> sm18;
		Node node = sm17.collect().get(0).getValue("?label");
		if(node.isLiteral()) {
			if(node.getLiteralValue().getClass().equals(BigDecimal.class) || node.getLiteralValue().getClass().equals(Double.class)){
				sm18 = sm17
					.sortPartition(new SM_OKS_Double("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(BigInteger.class) || node.getLiteralValue().getClass().equals(Integer.class)) {
				sm18 = sm17
					.sortPartition(new SM_OKS_Integer("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(Float.class)) {
				sm18 = sm17
					.sortPartition(new SM_OKS_Float("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(Long.class)){
				sm18 = sm17
					.sortPartition(new SM_OKS_Long("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else {
				sm18 = sm17
					.sortPartition(new SM_OKS_String("?label"), Order.ASCENDING)
					.setParallelism(1);
			}
		} else {
				sm18 = sm17
					.sortPartition(new SM_OKS_String("?label"), Order.ASCENDING)
					.setParallelism(1);
		}

		DataSet<SolutionMapping> sm19 = sm18
			.first(10);

		//************ Sink  ************
		sm19.writeAsText("./Query3-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		sm19.print();
	}
}