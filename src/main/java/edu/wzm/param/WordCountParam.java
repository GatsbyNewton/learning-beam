package edu.wzm.param;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

/**
 * <p>Usage: <code>WordCountParam --input=&lt;path&gt; --output=&lt;path&gt; --operator=&lt;operator&gt;</code><br>
 */
public class WordCountParam {
    private static final Logger LOGGER = LogManager.getLogger(WordCountParam.class);
    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(WordCountOptions.class);
        LOGGER.error("The Operator: {}", options.getOperator());

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from(options.getInput()))
                .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via(str -> Arrays.asList(str.split("\\W+"))))
                .apply(Flatten.iterables())
                .apply(Count.perElement())
                .apply(MapElements.into(TypeDescriptor.of(String.class))
                        .via(kv -> String.format("%s: %d", kv.getKey(), kv.getValue())))
                .apply(TextIO.write().to(options.getOutput()));

        pipeline.run().waitUntilFinish();
    }

    public interface WordCountOptions extends PipelineOptions {
        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInput();

        void setInput(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);

        @Default.String("Operator")
        String getOperator();

        void setOperator(String value);
    }
}
