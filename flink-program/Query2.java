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

public class Query2 {
	public static void main(String[] args) throws Exception {

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, "./bsbm/dataset.ttl");

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF(null, null, "?label"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www.w3.org/2000/01/rdf-schema#comment", null))
			.map(new T2SM_MF(null, null, "?comment"));

		DataSet<SolutionMapping> sm3 = sm1.cross(sm2)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm4 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer", null))
			.map(new T2SM_MF(null, null, "?p"));

		DataSet<SolutionMapping> sm5 = sm3.cross(sm4)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm6 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?p", null, "?producer"));

		DataSet<SolutionMapping> sm7 = sm5.join(sm6)
			.where(new SM_JKS(new String[]{"?p"}))
			.equalTo(new SM_JKS(new String[]{"?p"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm8 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://purl.org/dc/elements/1.1/publisher", null))
			.map(new T2SM_MF(null, null, "?p"));

		DataSet<SolutionMapping> sm9 = sm7.join(sm8)
			.where(new SM_JKS(new String[]{"?p"}))
			.equalTo(new SM_JKS(new String[]{"?p"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm10 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
			.map(new T2SM_MF(null, null, "?f"));

		DataSet<SolutionMapping> sm11 = sm9.cross(sm10)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm12 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?f", null, "?productFeature"));

		DataSet<SolutionMapping> sm13 = sm11.join(sm12)
			.where(new SM_JKS(new String[]{"?f"}))
			.equalTo(new SM_JKS(new String[]{"?f"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm14 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1", null))
			.map(new T2SM_MF(null, null, "?propertyTextual1"));

		DataSet<SolutionMapping> sm15 = sm13.cross(sm14)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm16 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual2", null))
			.map(new T2SM_MF(null, null, "?propertyTextual2"));

		DataSet<SolutionMapping> sm17 = sm15.cross(sm16)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm18 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual3", null))
			.map(new T2SM_MF(null, null, "?propertyTextual3"));

		DataSet<SolutionMapping> sm19 = sm17.cross(sm18)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm20 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new T2SM_MF(null, null, "?propertyNumeric1"));

		DataSet<SolutionMapping> sm21 = sm19.cross(sm20)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm22 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2", null))
			.map(new T2SM_MF(null, null, "?propertyNumeric2"));

		DataSet<SolutionMapping> sm23 = sm21.cross(sm22)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm24 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual4", null))
			.map(new T2SM_MF(null, null, "?propertyTextual4"));

		DataSet<SolutionMapping> sm25 = sm23.cross(sm24)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm26 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual5", null))
			.map(new T2SM_MF(null, null, "?propertyTextual5"));

		DataSet<SolutionMapping> sm27 = sm25.cross(sm26)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm28 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product51", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric4", null))
			.map(new T2SM_MF(null, null, "?propertyNumeric4"));

		DataSet<SolutionMapping> sm29 = sm27.cross(sm28)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm30 = sm29
			.map(new SM2SM_PF(new String[]{"?label", "?producer", "?productFeature", "?propertyTextual1", "?propertyTextual2", "?propertyTextual3", "?propertyNumeric1", "?propertyNumeric2", "?propertyTextual4", "?propertyTextual5", "?propertyNumeric4"}));

		//************ Sink  ************
		sm30.writeAsText("./Query2-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		sm30.print();
	}
}