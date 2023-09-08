import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public class DecisionTreeTry {
public static class DecisionTreeMapper extends Mapper<Object, Text, Text, Text> {
@Override
public void map(Object key, Text value, Context context) throws IOException,
InterruptedException {
// Parse input record and extract attributes and labels
String[] tokens = value.toString().split(",");
String label = tokens[tokens.length - 1]; // Assuming the label is the last column
// Emit attribute-value-label pairs
for (int i = 0; i < tokens.length - 1; i++) {
String attribute = "Attribute" + i;
String attributeValue = tokens[i];
context.write(new Text(attribute), new Text(attributeValue + ":" + label));
}
}
}
public static class DecisionTreeReducer extends Reducer<Text, Text, Text, Text> {
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws
IOException, InterruptedException {
// Collect attribute values and labels
Map<String, Map<String, Integer>> attributeCounts = new HashMap<>();
for (Text value : values) {
String[] parts = value.toString().split(":");
String attributeValue = parts[0];
String label = parts[1];
attributeCounts.computeIfAbsent(attributeValue, k -> new HashMap<>())
.merge(label, 1, Integer::sum);
}

// Calculate impurity or information gain and choose the best split
String bestSplit = calculateBestSplit(attributeCounts);
// Emit the chosen split point for the attribute
context.write(key, new Text(bestSplit));
}

private double calculateImpurity(Map<String, Integer> labelCounts) {
        // Implement your impurity calculation logic here
        // You should compute and return the impurity measure (e.g., Gini impurity or entropy)
        // Example:
        double impurity = 0.0;
        int totalSamples = 0;
        for (int count : labelCounts.values()) {
            totalSamples += count;
        }
        for (int count : labelCounts.values()) {
            double probability = (double) count / totalSamples;
            impurity += -probability * Math.log(probability);
        }
        return impurity;
    }

private String calculateBestSplit(Map<String, Map<String, Integer>> attributeCounts) {
    // Initialize variables to keep track of the best split and its quality
    String bestSplit = null; // The attribute value that results in the best split
    double bestImpurity = Double.MAX_VALUE; // Initialize with a large value

    // Iterate through each attribute value in attributeCounts
    for (Map.Entry<String, Map<String, Integer>> entry : attributeCounts.entrySet()) {
        String attributeValue = entry.getKey();
        Map<String, Integer> labelCounts = entry.getValue();

        // Calculate impurity for the current attribute split
        double impurity = calculateImpurity(labelCounts);

        // If the current impurity is better (lower) than the best so far, update bestSplit and bestImpurity
        if (impurity < bestImpurity) {
            bestImpurity = impurity;
            bestSplit = attributeValue;
        }
    }

    // Return the best attribute value to split on
    return bestSplit;
}

}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "DecisionTreeTry");
job.setJarByClass(DecisionTreeTry.class);
job.setMapperClass(DecisionTreeMapper.class);
job.setReducerClass(DecisionTreeReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
