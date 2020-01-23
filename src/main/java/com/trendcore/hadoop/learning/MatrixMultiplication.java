package com.trendcore.hadoop.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

    public static class Pair implements WritableComparable<Pair> {

        int i;
        int j;

        public Pair() {
            i = 0;
            j = 0;
        }

        public Pair(int i, int j) {
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
            return i + " " + j + " ";
        }
    }

    public static class MatrixMapperM extends Mapper<Object, Text, IntWritable, Element> {

        public MatrixMapperM() {
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");

            IntWritable writableKey = new IntWritable(Integer.parseInt(tokens[1]));

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

    public static class ReducerMxN extends Reducer<IntWritable, Element, Pair, DoubleWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Element> values, Context context) throws IOException, InterruptedException {
            ArrayList<Element> M = new ArrayList<>();
            ArrayList<Element> N = new ArrayList<>();

            Configuration conf = context.getConfiguration();

            for (Element element : values) {
                Element tempElement = ReflectionUtils.newInstance(Element.class, conf);
                ReflectionUtils.copy(conf, element, tempElement);

                if (tempElement.tag == 0) {
                    M.add(tempElement);
                } else if (tempElement.tag == 1) {
                    N.add(tempElement);
                }
            }

            for (int i = 0; i < M.size(); i++) {
                for (int j = 0; j < N.size(); j++) {
                    Pair p = new Pair(M.get(i).index, N.get(j).index);
                    double multiplyOutput = M.get(i).value * N.get(j).value;

                    context.write(p, new DoubleWritable(multiplyOutput));
                }
            }
        }
    }

    public static class MapMxN extends Mapper<Object, Text, Pair, DoubleWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println(key + " " + value);

            String readLine = value.toString();
            String[] pairValue = readLine.split(" ");

            Pair pair = new Pair(Integer.parseInt(pairValue[0]), Integer.parseInt(pairValue[1]));
            DoubleWritable doubleWritable = new DoubleWritable(Double.parseDouble(pairValue[2]));
            context.write(pair, doubleWritable);
        }
    }

    public static class ReduceMxN extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {

        @Override
        protected void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double d = 0;
            for (DoubleWritable value : values) {
                d = d + value.get();
            }
            System.out.println(key.i + " " + key.j + " " + d);
            context.write(key, new DoubleWritable(d));
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

        job.setReducerClass(ReducerMxN.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Element.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        String matrixMultiplicationIntermediateDir = homePath + "/matrix_multiplication/intermediate";
        String matrixMultiplicationOutputDir = homePath + "/matrix_multiplication/output";

        deleteDir(matrixMultiplicationIntermediateDir);
        deleteDir(matrixMultiplicationOutputDir);

        FileOutputFormat.setOutputPath(job, new Path(matrixMultiplicationIntermediateDir));

        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MapFinalOutput");
        job2.setJarByClass(MatrixMultiplication.class);

        job2.setMapperClass(MapMxN.class);
        job2.setReducerClass(ReduceMxN.class);

        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job2, new Path(matrixMultiplicationIntermediateDir));
        FileOutputFormat.setOutputPath(job2, new Path(matrixMultiplicationOutputDir));

        job2.waitForCompletion(true);
    }

    private static void deleteDir(String dir) {
        File file = new File(dir);
        if (file.exists()) {
            Arrays.stream(file.listFiles()).forEach(file1 -> file1.delete());
            file.delete();
        }
    }

}
