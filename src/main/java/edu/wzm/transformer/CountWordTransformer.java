package edu.wzm.transformer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountWordTransformer
        extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

    public static class ExtractWord extends SimpleFunction<String, List<String>> {
        @Override
        public List<String> apply(String input) {
            return Stream.of(input.split("\\W+"))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> input) {
        return input.apply(MapElements.via(new ExtractWord()))
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
                .apply(new CountWordTransformer())
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via(kv -> String.format("%s: %d", kv.getKey(), kv.getValue())))
                .apply(TextIO.write().to(output));

        pipeline.run().waitUntilFinish();
    }
}
