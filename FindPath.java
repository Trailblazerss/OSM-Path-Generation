import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.*;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.Tuple2;
import scala.Tuple3;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.fs.*;

import java.util.*;
import java.util.stream.Collectors;
import java.io.*;

public class FindPath {

	// From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
	private static double distance(double lat1, double lat2, double lon1, double lon2) {
		final int R = 6371; // Radius of the earth
		double latDistance = Math.toRadians(lat2 - lat1);
		double lonDistance = Math.toRadians(lon2 - lon1);
		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
				+ Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
				* Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = R * c * 1000; // convert to meters
		double height = 0; // For this assignment, we assume all locations have the same height.
		distance = Math.pow(distance, 2) + Math.pow(height, 2);
		return Math.sqrt(distance);
	}

	public static void main(String[] args) throws IOException{
		SparkSession spark = SparkSession.builder().getOrCreate();

		StructType newSchema = new StructType(new StructField[]{
				new StructField("_id", DataTypes.LongType, true, Metadata.empty()),
				new StructField("_lat", DataTypes.DoubleType, true, Metadata.empty()),
				new StructField("_lon", DataTypes.DoubleType, true, Metadata.empty()),
		});

		Dataset<Row> sf = spark.read()
				.format("xml")
				.option("rowTag", "node")
				.schema(newSchema)
				.load(args[0]);
		sf = sf.withColumn("id", col("_id")).select("id", "_lat", "_lon");
		sf.cache();

		List<StructField> ndfields = new ArrayList<StructField>();
		ndfields.add(DataTypes.createStructField("_ref", DataTypes.LongType, true));

		List<StructField> tagfields = new ArrayList<StructField>();
		tagfields.add(DataTypes.createStructField("_k", DataTypes.StringType, true));
		tagfields.add(DataTypes.createStructField("_v", DataTypes.StringType, true));

		StructType customSchema = new StructType(new StructField[]{
			new StructField("_id", DataTypes.LongType, true, Metadata.empty()),
			new StructField("nd", DataTypes.createArrayType(DataTypes.createStructType(ndfields)), true, Metadata.empty()),
			new StructField("tag", DataTypes.createArrayType(DataTypes.createStructType(tagfields)), true, Metadata.empty()),
		});

		Dataset<Row> df = spark.read()
				.format("xml")
				.option("rowTag", "way")
				.schema(customSchema)
				.load(args[0]);
		df.cache();

		df = df.withColumn("node", col("nd._ref"))
				.withColumn("tag", explode(col("tag")))
				.withColumn("tagk", col("tag._k"))
				.withColumn("tagv", col("tag._v"));
		df = df.select("_id", "node", "tagk", "tagv");

		df = df.filter((col("tagk").equalTo("highway"))
						.or(
								(col("tagk").equalTo("oneway")).and(col("tagv").equalTo("yes"))
						));

		df = df.groupBy("_id", "node").agg(collect_list("tagv").as("tagv1"));
		df = df.withColumn("Oneway", array_contains(col("tagv1"),"yes"))
				.select("_id", "node", "Oneway");
		df.cache();


		List<Row> lines = sf.select("id").collectAsList();
		List<Long> node = new ArrayList<>();
		List<Long> connect_node = new ArrayList<>();
		for(Row row : lines) {
			node.add(row.getLong(0));
		}

		List<Row> rows = sf.select("_lat", "_lon").collectAsList();
		List<List<Double>> info = new ArrayList<>();
		for(Row row : rows) {
			List<Double> lat_lon = new ArrayList<Double>();
			lat_lon.add(row.getDouble(0));
			lat_lon.add(row.getDouble(1));
			info.add(lat_lon);
		}

		HashMap<Long, List<Double>> node_info = new HashMap<Long, List<Double>>();
		for(int i=0; i<node.size();i++)
		{
			node_info.put(node.get(i), info.get(i));
		}

		List<Row> lines2 = df.collectAsList();
		List<List<Long>> edge = new ArrayList<>();
		List<Long> way_edges = new ArrayList<>();
		for(Row row : lines2) {
			way_edges = row.getList(1);
			if(row.getAs("Oneway").toString().equals("false"))
			{
				for(int i = 0; i < way_edges.size()-1; i++)
				{
					edge.add(Arrays.asList(way_edges.get(i), way_edges.get(i+1)));
					edge.add(Arrays.asList(way_edges.get(i+1), way_edges.get(i)));
					connect_node.add(way_edges.get(i));
					connect_node.add(way_edges.get(i+1));
				}
			}
			else
			{
				for(int i = 0; i < way_edges.size()-1; i++)
				{
					edge.add(Arrays.asList(way_edges.get(i), way_edges.get(i+1)));
					connect_node.add(way_edges.get(i));
					connect_node.add(way_edges.get(i+1));
				}
			}
		}

		HashSet h = new HashSet(connect_node);
		connect_node.clear();
		connect_node.addAll(h);

		Map<Long, List<Long>> adj_map_origin = new TreeMap<Long, List<Long>>();
		Map<Long, List<Long>> adj_map = new TreeMap<Long, List<Long>>();
		for(int i = 0; i < node.size(); i++)
		{
			adj_map_origin.put(node.get(i), new ArrayList<>());
		}
		for(int i = 0; i < edge.size(); i++)
		{
			List<Long> neighbors = adj_map_origin.get(edge.get(i).get(0));
			neighbors.add(edge.get(i).get(1));
			adj_map_origin.put(edge.get(i).get(0), neighbors);
		}
		for(long key : adj_map_origin.keySet())
		{
			Collections.sort(adj_map_origin.get(key));
		}
		for(long key : connect_node)
		{
			adj_map.put(key, adj_map_origin.get(key).stream().distinct().collect(Collectors.toList()));
		}

		String s;
		List<String> map = new ArrayList<>();
		for (Map.Entry<Long, List<Long>> entry : adj_map.entrySet())
		{
			List<String> neighborValue = entry.getValue().stream().map(String::valueOf).collect(Collectors.toList());
			String neighbor = String.join(" ", neighborValue);
			s = entry.getKey().toString() + " " + neighbor;
			map.add(s);
		}
		Dataset<String> map_dataset = spark.createDataset(map, Encoders.STRING());
		map_dataset.collect();
		map_dataset.coalesce(1).write().text("output1");


		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		List<Row> row_of_edg = new ArrayList<>();
		for(int i = 0; i < edge.size(); i++)
		{
			row_of_edg.add( RowFactory.create( edge.get(i).get(0), edge.get(i).get(1) ) );
		}
		JavaRDD<Row> edgRow = sc.parallelize(row_of_edg);

		StructType edgSchema = new StructType(new StructField[]{
				new StructField("src", DataTypes.LongType, true, Metadata.empty()),
				new StructField("dst", DataTypes.LongType, true, Metadata.empty()),
		});

		Dataset<Row> edgDF = spark.createDataFrame(edgRow, edgSchema);
		edgDF.cache();

		GraphFrame g = GraphFrame.apply(sf,edgDF);

		Dataset<Row> input_data = spark.read().text(args[1]);
		String line, vertex;
		List<String> mypath = new ArrayList<>();
		List<Row> test_input = input_data.collectAsList();
		for (Row row1: test_input)
		{
			line = row1.get(0).toString();
			String[] input = line.split(" ");
			Dataset<Row> path = g.bfs().fromExpr("id = " + input[0]).toExpr("id = " + input[1]).run();
			path.cache();
			List<Row> lines3 = path.collectAsList();
			for (Row row : lines3)
			{
				vertex = row.get(0).toString().split(",")[0].substring(1);
				for (int i = 2; i < row.size(); i = i + 2)
				{
					vertex = vertex + " -> " + row.get(i).toString().split(",")[0].substring(1);
				}
				mypath.add(vertex);
			}
		}

		Dataset<String> path_dataset = spark.createDataset(mypath, Encoders.STRING());
		path_dataset.collect();
		path_dataset.coalesce(1).write().text("output2");

		FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
		fs.mkdirs(new Path("output"));
		String file = fs.globStatus(new Path("output1/part*"))[0].getPath().getName();
		fs.rename(new Path("output1/"+file), new Path(args[2]));
		fs.delete(new Path("output1"), true);
		file = fs.globStatus(new Path("output2/part*"))[0].getPath().getName();
		fs.rename(new Path("output2/"+file), new Path(args[3]));
		fs.delete(new Path("output2"), true);
		fs.close();

		spark.stop();
	}
}

