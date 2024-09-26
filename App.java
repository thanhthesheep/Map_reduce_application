import java.io.IOException;
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

public class Project3 {
    
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();
    
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String line = value.toString();
            // Scanner scanner = new Scanner(line);

                // Split the line into fields using semicolon as the delimiter
                String[] fields = line.split(";");
                // Ensure that the line has at least 6 fields (0-based indexing)
                if (fields.length >= 6) {
                    String type = fields[1];
                    String yearString = fields[3];
                    String genresString = fields[5];
                    double rating = Double.parseDouble(fields[4]);
                    int firstgenre = 0;
    
                    // Extracting year
                    int year = Integer.parseInt(yearString);
                    // Checking predefined periods
                    if (year >= 1991 && year <= 2020 && rating >= 7.5 && type.equals("movie")) {
                        String period = getPeriod(year);
                        String[] genres = genresString.split(",");
                        if (genres.length > 1) {
                            Set<String> genreSet = new HashSet<String>(Arrays.asList(genres));
                            checkAndEmitGenre(genreSet, period, context);
                        }
                    }
                }
            }


                        // Function to determine period based on year
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

        private void checkAndEmitGenre(Set<String> genres, String period, Context context) throws IOException, InterruptedException{
            Set<String> actionThriller = new HashSet<>(Arrays.asList("Action", "Thriller"));
            Set<String> comedyRomance = new HashSet<>(Arrays.asList("Comedy", "Romance"));
            Set<String> adventureDrama = new HashSet<>(Arrays.asList("Adventure", "Drama"));

            if(genres.containsAll(actionThriller)){
                outputKey.set(period + "," + "Action;Thriller");
                context.write(outputKey, one);
            }
            if(genres.containsAll(comedyRomance)){
                outputKey.set(period + "," + "Comedy;Romance");
                context.write(outputKey, one);
            }
            if(genres.containsAll(adventureDrama)){
                outputKey.set(period + "," + "Adventure;Drama");
                context.write(outputKey, one);
            }
        }
    }
    

    

    public static class PeriodPartitioner extends Partitioner <Text , IntWritable>
    { //key = period,genre; value = one

        private static final Map<String, Integer> partitionMap = new HashMap<>();

            static {
                // Initialize partition mapping
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

        public int getPartition(Text key, IntWritable value, int numReducedTask)
        {
            // String line = key.toString();
            // String[] fields = line.split(",");
            // String period = fields[0];
            // String genre = fields[1];
            
            return partitionMap.getOrDefault((key.toString), numReducedTask -1);
           
        }
    }

    public static class GenreCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
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
        
        // // Delete output directory if it exists
        // Path outputPath = new Path(args[1]);
        // FileSystem fs = FileSystem.get(conf);
        // if (fs.exists(outputPath)) {
        //     fs.delete(outputPath, true);
        // }
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
