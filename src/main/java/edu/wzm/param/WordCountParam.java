package edu.wzm.param;

import org.apache.beam.sdk.options.*;

public class WordCount {
    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(WordCountOptions.class);
    }

    public interface WordCountOptions extends PipelineOptions {
        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }
}
