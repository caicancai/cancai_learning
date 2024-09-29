package org.dream.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Parallelism {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        env.addSource(new SourceFunction<Long>() {
            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(System.currentTimeMillis());
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        }).map((MapFunction<Long, Long>) aLong -> aLong / 1000).setParallelism(3).print();

        env.execute("Parallelism");
    }
}
