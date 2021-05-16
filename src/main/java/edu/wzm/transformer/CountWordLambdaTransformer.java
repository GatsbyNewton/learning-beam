package edu.wzm.transformer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

/**
 * 使用Lambda方式需要使用into()告诉MapElement，我们Mapper的返回值类型。
 */
public class CountWordLambdaTransformer
        extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> input) {
        return input.apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                    .via(str -> Arrays.asList(str.split("\\W+"))))
                .apply(Flatten.iterables())
                .apply(Count.perElement());
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: MinimalWordCount <input> <output>");
        }
        String input = args[0];
        String output = args[1];

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from(input))
                .apply(new CountWordLambdaTransformer())
                .apply(MapElements.into(TypeDescriptor.of(String.class))
                        .via(kv -> String.format("%s: %d", kv.getKey(), kv.getValue())))
                .apply(TextIO.write().to(output));

        pipeline.run().waitUntilFinish();
    }
}
