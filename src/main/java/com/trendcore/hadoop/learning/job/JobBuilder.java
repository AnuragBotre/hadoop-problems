package com.trendcore.hadoop.learning.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class JobBuilder {

    private MapReduce<Long, Long> mapReduce;

    private List<Consumer<Job>> inputFormat = new ArrayList<>();

    private List<Consumer<Job>> multipleInputFormat = new ArrayList<>();

    private Class jarClass;

    private Consumer<Job> outputPath;

    public static class KeyValue<K, V> {

        K k;
        V v;

        public KeyValue(K k, V v) {
            this.k = k;
            this.v = v;
        }
    }


    private static class MapReduce<K, V> {

        public MapReduce(K o, V o1) {

        }
    }

    private static class MapReduceBuilder {

        public <KEYIN, VALUEIN,KEYOUT,VALUEOUT> MapReduceBuilder map(Class<KEYIN> keyin,
                                                         Class<VALUEIN> valuein,
                                                         Class<KEYOUT> keyout,
                                                         Class<VALUEOUT> valueout,
                                                         Mapper<KEYIN, VALUEIN, KeyValue<KEYOUT, VALUEOUT>> mapper) {
            return new MapReduceBuilder();
        }

        public class Input<KEYIN, VALUEIN> {

            Class<KEYIN> k;
            Class<VALUEIN> v;

            public Input() {
            }

            public Input(Class<KEYIN> objectClass, Class<VALUEIN> textClass) {
                k = objectClass;
                v = textClass;
            }

            private <KEYOUT, VALUEOUT> MapResult<KEYOUT, VALUEOUT> map(Mapper<KEYIN, VALUEIN, KeyValue<KEYOUT, VALUEOUT>> mapper) {
                //this.mapper = mapper;
                System.out.println(mapper.getClass().getGenericSuperclass());
                return new MapResult<>(mapper);
            }


        }

        public interface Mapper<KEYIN, VALUEIN, R> {
            R map(KEYIN k, VALUEIN v);
        }

        public interface Reducer<KEYIN, VALUEIN, R> {
            R reduce(KEYIN k, Iterable<VALUEIN> v);
        }

        public <K,V> Input<K, V> mapInput(Class<K> objectClass, Class<V> textClass) {
            return new Input<>(objectClass,textClass);
        }

        private static class MapResult<KEYIN, VALUEIN> {
            private final Mapper mapper;

            private MapResult(Mapper mapper) {
                this.mapper = mapper;
            }

            public <KEYOUT, VALUEOUT> MapReduce<KEYOUT, VALUEOUT> reduce(Reducer<KEYIN, VALUEIN, KeyValue<KEYOUT, VALUEOUT>> reducer) {
                System.out.println();
                return new MapReduce<>(null, null);
            }
        }

        public static class InternalMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

            @Override
            protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
                super.map(key, value, context);
            }
        }
    }


    private JobBuilder addInput(Path path) {
        inputFormat.add(job -> addPath(path, job));
        return this;
    }

    private JobBuilder addMultipleInput(Path path, Class<? extends InputFormat> inputFormatClass) {
        multipleInputFormat.add(job -> MultipleInputs.addInputPath(job, path, inputFormatClass, MapReduceBuilder.InternalMapper.class));
        return this;
    }


    private void addPath(Path path, Job job) {
        try {
            FileInputFormat.addInputPath(job, path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JobBuilder output(Class<? extends OutputFormat> clazz) {
        return this;
    }

    private JobBuilder jarByClass(Class<?> jarClass) {
        this.jarClass = jarClass;
        return this;
    }

    public static void main(String[] args) {
        JobBuilder jobBuilder = new JobBuilder();
        //Path path, Class<? extends InputFormat > inputFormatClass

        String homePath = "/home/anurag/IdeaProjects/hadoop-learning";
        String m = homePath + "/input/matrix_multiplication_M";
        String n = homePath + "/input/matrix_multiplication_N";
        System.out.println(m);

        MapReduceBuilder mapReduceBuilder = new MapReduceBuilder();
        MapReduce<Long, Long> mapReduce = mapReduceBuilder
                .mapInput(Object.class, Text.class)
                .map((k, v) -> new KeyValue<String, String>(null, null))
                .reduce((k, v) -> new KeyValue<>(Long.parseLong(k), Long.parseLong("1")));

        JobBuilder jobBuilder1 = jobBuilder.addMultipleInput(new Path(m), TextInputFormat.class)
                .addMultipleInput(new Path(n), TextInputFormat.class)
                .jarByClass(JobBuilder.class)
                .mapReduce(mapReduce)
                .output(TextOutputFormat.class)
                .outputPath(null);
    }

    private JobBuilder outputPath(Path path) {
        outputPath = job -> FileOutputFormat.setOutputPath(job, path);
        return this;
    }

    private JobBuilder mapReduce(MapReduce<Long, Long> mapReduce) {
        this.mapReduce = mapReduce;
        return this;
    }

    private <K, V> MapReduceBuilder mapReduce(MapReduceBuilder mapReduce) {
        return null;
    }


}
