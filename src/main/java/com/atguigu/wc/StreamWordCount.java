package com.atguigu.wc;

import com.twitter.chill.Tuple2Serializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1、创建环境 及 配置环境参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 6789);
        // Properties props = new Properties();
        // props.setProperty("bootstrap.servers", "bigdata2:9092, bigdata3:9092,bigdata4:9092");
        // props.setProperty("group.id", "consumer1");
        // props.setProperty("auto.offset.rest", "latest");

        // DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), props));


        // 3、处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = streamSource.map(String::toUpperCase)
                .flatMap((String line, Collector<String> out) -> {
                    String[] fields = line.split(" ");
                    for (String field : fields) {
                        out.collect(field);
                    }
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOne.keyBy(t -> t.f0).sum(1);

        // 4、输出数据
        result.print();
        // Properties props2 = new Properties();
        // props2.setProperty("bootstrap.servers", "bigdata2:9092,bigdata3:9092,bigdata4:9092");
        // result.addSink(new FlinkKafkaProducer("topic2", new Tuple2Serializer<String, Integer>()., props2));

        // 5、运行代码
        env.execute();
    }
}
