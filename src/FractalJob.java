import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.logging.FileHandler;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
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

	public static class RasterMapper extends Mapper<LongWritable, LongWritable, IntWritable, IntWritable> {

		public void map(LongWritable begin, LongWritable end, Context context)
				throws IOException, InterruptedException {

			for (int x = (int) begin.get(); x < end.get(); x++) {
				for (int y = 0; y < imgSize; y++) {

					float hue = 0, brighness = 0;
					int icolor = 0;
					Complex z = new Complex(2.0 * (x - imgSize / 2) / (imgSize / 2),
							1.33 * (y - imgSize / 2) / (imgSize / 2));
					icolor = startCompute(generateZ(z), 0);
					if (icolor != -1) {
						brighness = 1f;
					}

					hue = (icolor % 256) / 255.0f;

					Color color = Color.getHSBColor(hue, 1f, brighness);
					context.write(new IntWritable(x + y * imgSize), new IntWritable(color.getRGB()));
				}
			}

		}
	}

	public static class ImageGenerateReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, IntWritable values, Context context)
				throws IOException, InterruptedException {
			context.write(key, values);
		}
	}

	private static Complex generateZ(Complex z) {
		return (z.times(z)).plus(constant);
	}

	private static int startCompute(Complex z, int color) {

		if (z.abs() > 4) {
			return color;
		} else if (color >= 256) {
			return -1;
		} else {
			color = color + 1;
			return startCompute(generateZ(z), color);
		}
	}

	public static void generateFractal(int mapNr, Path filePath, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf);
		job.setJobName(FractalJob.class.getSimpleName());

		job.setJarByClass(FractalJob.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(RasterMapper.class);

		job.setReducerClass(ImageGenerateReducer.class);
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
				
				final LongWritable begin = new LongWritable(offset);
				final LongWritable end = new LongWritable(offset + imgSize / mapNr);
				offset = (int) end.get();
				
				@SuppressWarnings("deprecation")
				final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file, LongWritable.class,
						LongWritable.class, CompressionType.NONE);
				try {
					writer.append(begin, end);
					writer.hflush();
				} catch (Exception e){ 
						e.printStackTrace();
				}finally {
					writer.close();
				}
				System.out.println("Wrote input for Map #" + i);
			}

			System.out.println("Starting Job");
			final long startTime = System.currentTimeMillis();
			job.waitForCompletion(true);
			final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
			

			// read outputs
			System.out.println("Constructing image...");
			IntWritable pxLoc = new IntWritable();
			IntWritable color = new IntWritable();

			Path file = new Path(output, "part-r-00000");
			@SuppressWarnings("deprecation")
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			try {
				BufferedImage image = new BufferedImage(imgSize, imgSize, BufferedImage.TYPE_INT_RGB);
				while (reader.next(pxLoc, color)) {
					
					for (int i = 0; i < imgSize; i++)
						image.setRGB(i, (pxLoc.get() - i) / imgSize, color.get());
				}
				System.out.println(image.toString());
				ImageIO.write((RenderedImage) image, "jpg", new File(FractalJob.outputpath));
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			 finally {
				reader.close();
			}
			System.out.println("Job Finished in " + duration + " seconds");
		} finally {
			
			fs.delete(filePath, true);

		}
		

	}

	public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		if (args.length != 5) {
			System.err.println("Usage: " + getClass().getName() + " <Maps> <ImageSize> <Complex Re> <Complex Im> <Output path>");
			ToolRunner.printGenericCommandUsage(System.err);
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

		System.exit(ToolRunner.run(null, new FractalJob(), args));
	}

}
