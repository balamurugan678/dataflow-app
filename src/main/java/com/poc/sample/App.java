package com.poc.sample;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class App {
    public static void main(String[] args) {


        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(WordCountOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> fileReadCollection = pipeline.apply(TextIO.read().from(options.getInputFile()));
        PCollection<String> extractWords = fileReadCollection.apply("ExtractWords", FlatMapElements
                .into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))));
        PCollection<String> filteredWord = extractWords.apply(
                Filter.by((String word) -> !word.isEmpty()));
        PCollection<KV<String, Long>> countedCollection = filteredWord.apply(Count.<String>perElement());
        PCollection<String> formattedResults = countedCollection.apply("FormatResults", MapElements
                .into(TypeDescriptors.strings())
                .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()));
        PDone wordcounts = formattedResults.
                apply(TextIO.write().to("gs://***/counts"));


        pipeline.run().waitUntilFinish();

    }
}
