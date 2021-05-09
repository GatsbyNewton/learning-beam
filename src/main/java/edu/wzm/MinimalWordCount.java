package edu.wzm;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

/**
 * @author: wangzhiming
 * @Date: 2020/3/28
 * @version:
 * @Description:
 */
public class MinimalWordCount {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: MinimalWordCount <input> <output>");
        }

        String input = args[0];
        String output = args[1];

        PipelineOptions options = PipelineOptionsFactory.create();

//        options.setRunner(PipelineOptions.DirectRunner.class); // 显式指定PipelineRunner：DirectRunner（Local模式）

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(input)) // 读取本地文件，构建第一个PTransform
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() { // 对文件中每一行进行处理（实际上Split）

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split("[\\s:\\,\\.\\-]+")) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }

                }))
                .apply(Count.perElement()) // 统计每一个Word的Count
                .apply("ConcatResultKVs", MapElements.via( // 拼接最后的格式化输出（Key为Word，Value为Count）
                        new SimpleFunction<KV<String, Long>, String>() {

                            @Override
                            public String apply(KV<String, Long> input) {
                                return input.getKey() + ": " + input.getValue();
                            }

                        }))
                .apply(TextIO.write().to(output)); // 输出结果

        pipeline.run().waitUntilFinish();
    }

}
