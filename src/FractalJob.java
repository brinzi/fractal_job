import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.FileHandler;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FractalJob extends Configured implements Tool {

	static private final String TMP_DIR_PREFIX = FractalJob.class.getSimpleName();
	static private Complex constant;
	static private int imgSize;
	static private String outputpath;

	static FileHandler fh;

	public static class RasterMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
		private int imageS;
		private static Complex mapConstant;
		

		@Override
		public void setup(Context context) throws IOException {
			imageS = context.getConfiguration().getInt("image.size", -1);
			
			mapConstant = new Complex(context.getConfiguration().getDouble("constant.re", -1),
					context.getConfiguration().getDouble("constant.im", -1));

		}

		@Override
		public void map(IntWritable begin, IntWritable end, Context context) throws IOException, InterruptedException {
		
		
			for (int x = (int) begin.get(); x < end.get(); x++) {
				for (int y = 0; y < imageS; y++) {

					float hue = 0, brighness = 0;
					int icolor = 0;
					Complex z = new Complex(2.0 * (x - imageS / 2) / (imageS / 2),
							1.33 * (y - imageS / 2) / (imageS / 2));
					
					icolor = startCompute(generateZ(z), 0);
				
					if (icolor != -1) {
						brighness = 1f;
					}
					
					
					hue = (icolor % 256) / 255.0f;
					
					Color color = Color.getHSBColor(hue, 1f, brighness);
					try {
						context.write(new IntWritable(x + y * imageS), new IntWritable(color.getRGB()));
					} catch (Exception e) {
						e.printStackTrace();
					
					}
				
				}
			}

		}
		

		private static Complex generateZ(Complex z) {
			return (z.times(z)).plus(mapConstant);
		}

		private static int startCompute(Complex z, int color) {

			if (z.abs() > 4) {
				return color;
			} else if (color >= 255) {
				return -1;
			} else {
				color = color + 1;
				return startCompute(generateZ(z), color);
			}
		}

	}
	
	public static class ImageReducer extends Reducer<IntWritable, IntWritable, WritableComparable<?>, Writable> {
		private SequenceFile.Writer writer;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			writer.close();
		}
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Path outDir = new Path(conf.get(FileOutputFormat.OUTDIR));
			Path outFile = new Path(outDir, "pixels-out");
		
			Option optPath = SequenceFile.Writer.file(outFile);
			Option optKey = SequenceFile.Writer.keyClass(IntWritable.class);
			Option optVal = SequenceFile.Writer.valueClass(IntWritable.class);
			Option optCom = SequenceFile.Writer.compression(CompressionType.NONE);
			try {
				writer = SequenceFile.createWriter(conf, optCom, optKey, optPath, optVal);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reduce (IntWritable key,  Iterable<IntWritable>  value, Context context) throws IOException, InterruptedException {
			
				try{
					
					writer.append(key, value.iterator().next());
				} catch (Exception e) {
						e.printStackTrace();
						
				}
			}
		}
	public static void generateFractal(int mapNr, Path filePath, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
				
		conf.setInt("image.size", imgSize);
		conf.setDouble("constant.re", FractalJob.constant.re());
		conf.setDouble("constant.im", FractalJob.constant.im());

		Job job = Job.getInstance(conf);
		job.setJobName(FractalJob.class.getSimpleName());

		job.setJarByClass(FractalJob.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(RasterMapper.class);

		job.setReducerClass(ImageReducer.class);
		job.setNumReduceTasks(1);

		job.setSpeculativeExecution(false);

		final Path input = new Path(filePath, "in");
		final Path output = new Path(filePath, "out");

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		final FileSystem fs = FileSystem.get(conf);
		if (fs.exists(filePath)) {
			throw new IOException(
					"Directory " + fs.makeQualified(filePath) + " already exists.  Please remove it first.");
		}
		if (!fs.mkdirs(input)) {
			throw new IOException("Cannot create input directory " + input);
		}

		try {
			int offset = 0;
		
			// generate an input file for each map task
			for (int i = 0; i < mapNr; ++i) {

				final Path file = new Path(input, "part" + i);

				final IntWritable begin = new IntWritable(offset);
				final IntWritable end = new IntWritable(offset + imgSize / mapNr);
				offset = (int) end.get();

				Option optPath = SequenceFile.Writer.file(file);
				Option optKey = SequenceFile.Writer.keyClass(IntWritable.class);
				Option optVal = SequenceFile.Writer.valueClass(IntWritable.class);
				Option optCom = SequenceFile.Writer.compression(CompressionType.NONE);
				SequenceFile.Writer writer = SequenceFile.createWriter(conf, optCom, optKey, optPath, optVal);
				try {
					writer.append(begin, end);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					writer.close();
				}
				System.out.println("Wrote input for Map #" + i);
			}

			System.out.println("Starting Job");
			final long startTime = System.currentTimeMillis();
		      job.waitForCompletion(true);
			
			
			
			System.out.println("Constructing image...");

			Path file = new Path(output, "pixels-out");

			org.apache.hadoop.io.SequenceFile.Reader.Option path = SequenceFile.Reader.file(file);

			SequenceFile.Reader reader = new SequenceFile.Reader(conf, path);
			IntWritable pxLoc = new IntWritable();
			IntWritable color = new IntWritable();
			Map<Integer, Integer> colors = new HashMap<Integer,Integer>();
			try {
				BufferedImage image = new BufferedImage(imgSize, imgSize, BufferedImage.TYPE_INT_RGB);
		
				while (reader.next(pxLoc, color)) {
						colors.put(pxLoc.get(), color.get());
							
				}
			
				for (int i = 0; i < imgSize; i++) {
					for (int j = 0; j < imgSize; j++) {
						image.setRGB(i, j, colors.get(i+j*imgSize));
					}
				}
				

				
				ImageIO.write( (RenderedImage) image, "png", new File(FractalJob.outputpath));
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				reader.close();
			}
			final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
			System.out.println("Job Finished in " + duration + " seconds");
		} finally {
		fs.delete(filePath, true);
		
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			System.err
					.println("Usage: " + "fractal.jar" + " <Maps> <ImageSize> <Complex Re> <Complex Im> <Output path>");
			return 2;
		}

		final int mapNr = Integer.parseInt(args[0]);
		FractalJob.imgSize = Integer.parseInt(args[1]);
		FractalJob.constant = new Complex(Double.parseDouble(args[2]), Double.parseDouble(args[3]));
		FractalJob.outputpath = args[4];

		long now = System.currentTimeMillis();
		int rand = new Random().nextInt(Integer.MAX_VALUE);
		final Path tmpDir = new Path(TMP_DIR_PREFIX + "_" + now + "_" + rand);

		System.out.println("Number of Maps  = " + mapNr);
		generateFractal(mapNr, tmpDir, getConf());
		return 0;
	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new Configuration(), new FractalJob(), args));

	}

}
