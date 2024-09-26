import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class Project3 {
    // Mapper class
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        // Map function, run when mapping phase starts
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            // Transfer the input value to string
            String line = value.toString();
            // Split the string by ";" for each line of data, creating 6 fields: id, type, title, year, rating, genres
            String[] fields = line.split(";");
            // if-statements to check if the inputs are valid
            if (fields.length >= 6) {
                String type = fields[1];
                String yearString = fields[3];
                String genresString = fields[5];
                
                // Convert the rating to double
                double rating = Double.parseDouble(fields[4]);

                // In the dataset, some of the year values are missing, so we need to check if the year is missing
                if (yearString.equals("\\N")) {
                    return;
                }
                // After we make sure that the year is not missing, we convert the year to integer
                int year = Integer.parseInt(yearString);

                // Check if the year is between 1991 and 2020, the rating is greater than or equal to 7.5, and the type is movie
                if (year >= 1991 && year <= 2020 && rating >= 7.5 && type.equals("movie")) {
                    // Get the period of the movie
                    String period = getPeriod(year);
                    // Split the genres input by ","
                    String[] genres = genresString.split(",");
                    //Filter out the movies with only one genre
                    if (genres.length > 1) {
                        // Create a set of genres
                        Set<String> genreSet = new HashSet<String>(Arrays.asList(genres));
                        // Check and write the genres and period to the <key, value> pair
                        checkAndEmitGenre(genreSet, period, context);
                    }
                }
            }
        }
        /
        /*This method get the year and classify movies into periods. Never each the last else because we filter out
         * movies with year outside the range 1991-2020 in the map function.
         */
        private String getPeriod(int year) {
            if (year >= 1991 && year <= 2000) {
                return "[1991-2000]";
            } else if (year >= 2001 && year <= 2010) {      
                return "[2001-2010]";
            } else if (year >= 2011 && year <= 2020) {
                return "[2011-2020]";
            } else {
                return "Outside period 1991-2020";
            }
        }
        // Function to check and write the genres and period to the <key, value> pair
        private void checkAndEmitGenre(Set<String> genres, String period, Context context) throws IOException, InterruptedException {
           /*Create a set of required genres. Then check if the set passed from outside contains the
            * required genres. If it does, write the period and genres to the key.
            */
            Set<String> actionThriller = new HashSet<>(Arrays.asList("Action", "Thriller"));
            Set<String> comedyRomance = new HashSet<>(Arrays.asList("Comedy", "Romance"));
            Set<String> adventureDrama = new HashSet<>(Arrays.asList("Adventure", "Drama"));

            if (genres.containsAll(actionThriller)) {
                outputKey.set(period + "," + "Action;Thriller");
                context.write(outputKey, one);
            }
            if (genres.containsAll(comedyRomance)) {
                outputKey.set(period + "," + "Comedy;Romance");
                context.write(outputKey, one);
            }
            if (genres.containsAll(adventureDrama)) {
                outputKey.set(period + "," + "Adventure;Drama");
                context.write(outputKey, one);
            }
        }
    }

    // Partitioner class starts
    public static class PeriodPartitioner extends Partitioner<Text, IntWritable> {

        private static final Map<String, Integer> partitionMap = new HashMap<>();
        /* Initialize the partitionMap, partition into 9 parts, 3 periods and 3 genres combinations.
        <key, value> pairs are the period and genre combinations, and the value is the partition number. 
        */
        static {
            partitionMap.put("[1991-2000],Action;Thriller", 0);
            partitionMap.put("[1991-2000],Comedy;Romance", 1);
            partitionMap.put("[1991-2000],Adventure;Drama", 2);
            partitionMap.put("[2001-2010],Action;Thriller", 3);
            partitionMap.put("[2001-2010],Comedy;Romance", 4);
            partitionMap.put("[2001-2010],Adventure;Drama", 5);
            partitionMap.put("[2011-2020],Action;Thriller", 6);
            partitionMap.put("[2011-2020],Comedy;Romance", 7);
            partitionMap.put("[2011-2020],Adventure;Drama", 8);
        }

        @Override
        /* Get the partition number based on the key
        -> return the partition number if the key is in the partitionMap, otherwise return the last partition number*/
        public int getPartition(Text key, IntWritable value, int numReducedTasks) {
            return partitionMap.getOrDefault(key.toString(), numReducedTasks - 1);
        }
    }
    //Reducer class starts
    public static class GenreCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Create a new IntWritable object
        private IntWritable result = new IntWritable();
        // Reduce function, run when reducing phase starts
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Sum up the values which is count the number of entries from each partition
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            // Write the result to the context
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "project3");
        job.setJarByClass(Project3.class);
        job.setMapperClass(MovieMapper.class);
        job.setCombinerClass(GenreCountReducer.class);
        job.setPartitionerClass(PeriodPartitioner.class);
        job.setReducerClass(GenreCountReducer.class);
        job.setNumReduceTasks(9);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /* */
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

