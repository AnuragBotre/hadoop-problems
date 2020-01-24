package com.trendcore.hadoop.learning.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobBuilder {

    private MapReduce<Long, Long> mapReduce;

    public static class KeyValue<K, V> {

        K k;
        V v;

        public KeyValue(K k, V v) {
            this.k = k;
            this.v = v;
        }
    }


    private static class MapReduce<K,V>{

        public MapReduce(K o, V o1) {

        }
    }

    private static class MapReduceBuilder {

        public class Input<KEYIN, VALUEIN>{

            private <KEYOUT, VALUEOUT> MapResult<KEYOUT, VALUEOUT> map(Mapper<KEYIN, VALUEIN, KeyValue<KEYOUT, VALUEOUT>> mapper) {
                //this.mapper = mapper;
                return new MapResult(mapper);
            }
        }

        public interface Mapper<KEYIN, VALUEIN, R> {
            R map(KEYIN k, VALUEIN v);
        }

        public interface Reducer<KEYIN, VALUEIN, R> {
            R reduce(KEYIN k, Iterable<VALUEIN> v);
        }

        public <K,V> Input<K, V> input(Class<K> objectClass, Class<V> textClass) {
            return new Input<>();
        }

        private static class MapResult<KEYIN, VALUEIN> {
            private final Mapper mapper;

            private MapResult(Mapper mapper) {
                this.mapper = mapper;
            }

            public <KEYOUT, VALUEOUT> MapReduce<KEYOUT, VALUEOUT> reduce(Reducer<KEYIN, VALUEIN, KeyValue<KEYOUT, VALUEOUT>> reducer) {
                return new MapReduce<>(null, null);
            }

        }
    }


    private JobBuilder addInput(Path path, Class<? extends InputFormat> inputFormatClass) {
        return this;
    }

    private JobBuilder addMultipleInput(Path path, Class<? extends InputFormat> inputFormatClass) {
        return this;
    }

    private JobBuilder output(Class<? extends OutputFormat> clazz) {
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
                .input(Object.class, Text.class)
                .map((k, v) -> new KeyValue<String, String>(null, null))
                .reduce((k, v) -> new KeyValue<>(Long.parseLong(k), Long.parseLong("1")));

        JobBuilder jobBuilder1 = jobBuilder.addMultipleInput(new Path(m), TextInputFormat.class)
                .addMultipleInput(new Path(n), TextInputFormat.class)
                .mapReduce(mapReduce)
                .output(TextOutputFormat.class)
                .outputPath(new Path(""));
    }

    private JobBuilder outputPath(Path path) {
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
