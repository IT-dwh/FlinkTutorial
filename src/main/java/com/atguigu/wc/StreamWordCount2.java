package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;

import javax.swing.*;
import java.util.Properties;

public class StreamWordCount2 {
    public static void main(String[] args) throws Exception {
        // 1、构造执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "bigdata2:9092,bigdata3:9092,bigdata4:9092");
        props.setProperty("group.id", "consumer20220601");
        props.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("topic1", new SimpleStringSchema(), props));



        // 3、处理数据
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = kafkaSource.map(line -> line.toUpperCase()).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                collector.collect(s);
            }
        });



        // 简单聚合：sum,min,max等， 只能做简单聚合
        // reduce是更加一般化的聚合, 例如：不是要把每个1进行sum累加， 而是要进行拼接, 这样sum方法就不行了, 就需要reduce方法了， 它更加通用，完全可以实现sum或者min的功能！
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = stringSingleOutputStreamOperator.map(t -> Tuple2.of(t, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(t -> t.f0);


        // 第一种reduce函数的写法：用匿名内部类
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = tuple2StringKeyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });

        reduce.print("reduce->");
        // 第二种reduce函数的写法: 用lambda表达是
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce1 = tuple2StringKeyedStream.reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));
        reduce1.print("reduce1->");
        // 第三种reduce函数的写法: 采用富函数, 可以给累加器赋初始值
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce2 = tuple2StringKeyedStream.reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });
        reduce2.print("reduce2->");

        // 4、sink
        // https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/
        // FlinkKafkaProducer is deprecated and will be removed with Flink 1.15, please use KafkaSink instead.
        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "bigdata2:9092,bigdata3:9092,bigdata4:9092");
        SingleOutputStreamOperator<String> result = reduce1.map(t -> t.f0 + "=" + t.f1);
        result.addSink(new FlinkKafkaProducer<String>("topic2", new SimpleStringSchema(), props2));

        // 5、启动任务
        env.execute();
    }
}
