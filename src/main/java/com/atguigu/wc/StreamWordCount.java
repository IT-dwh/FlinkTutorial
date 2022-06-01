package com.atguigu.wc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1、创建环境 及 配置环境参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、读取数据
        // test1：从socket读取
        // DataStreamSource<String> streamSource = env.socketTextStream("localhost", 6789);

        // test2：从kafka读取
        // https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "bigdata2:9092, bigdata3:9092,bigdata4:9092");
        props.setProperty("group.id", "consumer1");
        props.setProperty("auto.offset.rest", "latest");
        // FlinkKafkaConsumer is deprecated and will be removed with Flink 1.15, please use KafkaSource instead.
        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), props));

        // 3、处理数据
        // 3.1、map 方法，输入一个map函数，函数的第一个泛型是刚刚从kafka读取的String类型，输出类型可以是任意类型,甚至可以把输入的字符串构造成一个POJO类型
        // 返回SingleOutputStreamOperator对象， 这个对象的泛型，就是mapFunction的输出类型
        SingleOutputStreamOperator<String> mapStream = kafkaSource.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });

        // 3.2、filterFunction，只需要一个泛型，即输入类型， 它只是过滤， 输入什么类型，输出就什么类型，只是把一部分数据过滤掉而已！
        SingleOutputStreamOperator<String> filterStream = mapStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (s.equals("hello")) {
                    return false;
                }
                return true;
            }
        });

        // 3.3、flatMapFunction, 其实可以等价于map, 也可以等价于filter
        // 如果输入1个内容，输出多个内容， 则为flatMap本身
        // 如果输入1个内容，输出1个内容，则等价map
        // 如果输入1个内容，输出0个内容， 则等价于filter
        // 它跟map一样，输入类型来自于上游和输出类型可以自己定义
        // 它跟map不一样的是，它输入1个，可能输出多个，所以只能使用collector来收集起来再发送到下游！而不像map直接使用return就行了！
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = filterStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(Tuple2.of(s, 1));
            }
        });

        // 3.4、keyBy是按照key进行分组， 传入一个key的选择器，告诉数据流怎么划分，
        // 它的返回类型是： 元素 + key
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(tuple -> tuple.f0);

        // 3.5、sum聚合，只能传入下标或者字段儿名, 它的泛型是tuple2<String, Integer>, 所以给他传一个f1字段儿
        // 聚合后的数据类型就又回到了DataStream
        // 返回的还是与原先一样的数据类型, 只不过某个字段儿值是累加过后的！
        SingleOutputStreamOperator<Tuple2<String, Integer>> f1 = tuple2StringKeyedStream.sum("f1");

        // 4、输出结果
        f1.print();

        // 5、运行代码
        env.execute();
    }
}
