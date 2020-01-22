package com.trendcore.hadoop.learning;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class MatrixMultiplication {

    public static class Element implements Writable {

        int tag;
        int index;
        double value;

        public Element() {
        }

        public Element(int tag, int index, double value) {
            this.tag = tag;
            this.index = index;
            this.value = value;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(tag);
            dataOutput.writeInt(index);
            dataOutput.writeDouble(value);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            tag = dataInput.readInt();
            index = dataInput.readInt();
            value = dataInput.readDouble();
        }
    }

    class Pair implements WritableComparable<Pair> {

        int i;
        int j;

        public Pair() {
            this.i = i;
            this.j = j;
        }

        @Override
        public int compareTo(Pair o) {
            if (i > o.i) {
                return 1;
            } else if (i < o.i) {
                return -1;
            } else {
                if (j > o.j) {
                    return 1;
                } else if (j < o.j) {
                    return -1;
                }
            }

            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(i);
            dataOutput.writeInt(j);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            i = dataInput.readInt();
            j = dataInput.readInt();
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "i=" + i +
                    ", j=" + j +
                    '}';
        }
    }

    public static class MatrixMapperM extends Mapper<Object, Text, IntWritable, Element> {

        public MatrixMapperM() {
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");

            IntWritable writableKey = new IntWritable(Integer.parseInt(tokens[0]));

            Element element = new Element(0,
                    Integer.parseInt(tokens[0]),
                    Double.parseDouble(tokens[2]));

            System.out.println("MatrixMapperM " + writableKey + " :: " + element.tag + " " + element.index + " " + element.value);
            context.write(writableKey, element);
        }
    }

    public static class MatrixMapperN extends Mapper<Object, Text, IntWritable, Element> {

        public MatrixMapperN() {
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");

            IntWritable writableKey = new IntWritable(Integer.parseInt(tokens[0]));

            Element element = new Element(1,
                    Integer.parseInt(tokens[1]),
                    Double.parseDouble(tokens[2]));

            System.out.println("MatrixMapperN " + writableKey + ":: " + element.tag + " " + element.index + " " + element.value);
            context.write(writableKey, element);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJobName("MapIntermediate");
        job.setJarByClass(MatrixMultiplication.class);

        String homePath = "/home/anurag/IdeaProjects/hadoop-learning";
        String m = homePath + "/input/matrix_multiplication_M";
        String n = homePath + "/input/matrix_multiplication_N";
        System.out.println(m);

        MultipleInputs.addInputPath(job, new Path(m), TextInputFormat.class, MatrixMapperM.class);
        MultipleInputs.addInputPath(job, new Path(n), TextInputFormat.class, MatrixMapperN.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Element.class);

        job.setOutputKeyClass(Pair.class);

        String maxtrixMultiplicationOutputDir = homePath + "/matrix_multiplication_output";
        deleteDir(maxtrixMultiplicationOutputDir);
        FileOutputFormat.setOutputPath(job, new Path(maxtrixMultiplicationOutputDir));

        job.waitForCompletion(true);

    }

    private static void deleteDir(String dir) {
        File file = new File(dir);
        if (file.exists()) {
            Arrays.stream(file.listFiles()).forEach(file1 -> file1.delete());
            file.delete();
        }
    }

}
