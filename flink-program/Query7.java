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

public class Query7 {
	public static void main(String[] args) throws Exception {

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, "./bsbm/dataset.ttl");

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product71", "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF(null, null, "?productLabel"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product71"))
			.map(new T2SM_MF("?offer", null, null));

		DataSet<SolutionMapping> sm3 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price", null))
			.map(new T2SM_MF("?offer", null, "?price"));

		DataSet<SolutionMapping> sm4 = sm2.join(sm3)
			.where(new SM_JKS(new String[]{"?offer"}))
			.equalTo(new SM_JKS(new String[]{"?offer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm5 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor", null))
			.map(new T2SM_MF("?offer", null, "?vendor"));

		DataSet<SolutionMapping> sm6 = sm4.join(sm5)
			.where(new SM_JKS(new String[]{"?offer"}))
			.equalTo(new SM_JKS(new String[]{"?offer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm7 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?vendor", null, "?vendorTitle"));

		DataSet<SolutionMapping> sm8 = sm6.join(sm7)
			.where(new SM_JKS(new String[]{"?vendor"}))
			.equalTo(new SM_JKS(new String[]{"?vendor"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm9 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country", "http://downlode.org/rdf/iso-3166/countries#GB"))
			.map(new T2SM_MF("?vendor", null, null));

		DataSet<SolutionMapping> sm10 = sm8.join(sm9)
			.where(new SM_JKS(new String[]{"?vendor"}))
			.equalTo(new SM_JKS(new String[]{"?vendor"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm11 = dataset
			.filter(new T2T_FF(null, "http://purl.org/dc/elements/1.1/publisher", null))
			.map(new T2SM_MF("?offer", null, "?vendor"));

		DataSet<SolutionMapping> sm12 = sm10.join(sm11)
			.where(new SM_JKS(new String[]{"?offer", "?vendor"}))
			.equalTo(new SM_JKS(new String[]{"?offer", "?vendor"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm13 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo", null))
			.map(new T2SM_MF("?offer", null, "?date"));

		DataSet<SolutionMapping> sm14 = sm12.join(sm13)
			.where(new SM_JKS(new String[]{"?offer"}))
			.equalTo(new SM_JKS(new String[]{"?offer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm15 = sm1.cross(sm14)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm16 = sm15
			.filter(new SM2SM_FF("(> ?date \"2008-05-01T00:00:00\")"));

		DataSet<SolutionMapping> sm17 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product71"))
			.map(new T2SM_MF("?review", null, null));

		DataSet<SolutionMapping> sm18 = dataset
			.filter(new T2T_FF(null, "http://purl.org/stuff/rev#reviewer", null))
			.map(new T2SM_MF("?review", null, "?reviewer"));

		DataSet<SolutionMapping> sm19 = sm17.join(sm18)
			.where(new SM_JKS(new String[]{"?review"}))
			.equalTo(new SM_JKS(new String[]{"?review"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm20 = dataset
			.filter(new T2T_FF(null, "http://xmlns.com/foaf/0.1/name", null))
			.map(new T2SM_MF("?reviewer", null, "?revName"));

		DataSet<SolutionMapping> sm21 = sm19.join(sm20)
			.where(new SM_JKS(new String[]{"?reviewer"}))
			.equalTo(new SM_JKS(new String[]{"?reviewer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm22 = dataset
			.filter(new T2T_FF(null, "http://purl.org/dc/elements/1.1/title", null))
			.map(new T2SM_MF("?review", null, "?revTitle"));

		DataSet<SolutionMapping> sm23 = sm21.join(sm22)
			.where(new SM_JKS(new String[]{"?review"}))
			.equalTo(new SM_JKS(new String[]{"?review"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm24 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1", null))
			.map(new T2SM_MF("?review", null, "?rating1"));

		DataSet<SolutionMapping> sm25 = sm23.leftOuterJoin(sm24)
			.where(new SM_JKS(new String[]{"?review"}))
			.equalTo(new SM_JKS(new String[]{"?review"}))
			.with(new SM_LOJF());

		DataSet<SolutionMapping> sm26 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2", null))
			.map(new T2SM_MF("?review", null, "?rating2"));

		DataSet<SolutionMapping> sm27 = sm25.leftOuterJoin(sm26)
			.where(new SM_JKS(new String[]{"?review"}))
			.equalTo(new SM_JKS(new String[]{"?review"}))
			.with(new SM_LOJF());

		DataSet<SolutionMapping> sm28 = sm16.cross(sm27)
			.with(new SM_CF());

		DataSet<SolutionMapping> sm29 = sm28
			.map(new SM2SM_PF(new String[]{"?productLabel", "?offer", "?price", "?vendor", "?vendorTitle", "?review", "?revTitle", "?reviewer", "?revName", "?rating1", "?rating2"}));

		//************ Sink  ************
		sm29.writeAsText("./Query7-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		sm29.print();
	}
}