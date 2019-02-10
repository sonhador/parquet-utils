package parquet_utils.parquet_metadata_generator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;

public class App {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Correct Usage: ./parquet-metadata-generator.jar <hdfs://..|file:///..>");
			System.exit(1);
		}
		
		Path dir = new Path(args[0]);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		FileSystem fs = dir.getFileSystem(conf);
		
		ParquetFileWriter.writeMetadataFile(conf, dir, ParquetFileReader.readAllFootersInParallel(conf, fs.getFileStatus(dir)));
	}
}
