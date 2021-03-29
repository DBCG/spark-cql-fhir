package org.opencds.cqf.cql.spark.sample;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.opencds.cqf.cql.evaluator.fhir.DirectoryBundler;
import org.opencds.cqf.cql.spark.sample.transform.EvaluatorMapPartitionsFunction;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;

public class Main {

    public static void main(String[] args) throws IOException {
        String outputPath = "output";

        if (Files.exists(Paths.get(outputPath))) {
            Files.walk(Paths.get(outputPath)).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
     
        String resourcePath = "../connectathon/fhir401/input/tests/measure/EXM74-10.2.000";
        String cqlPath = "../connectathon/fhir401/input/pagecontent/cql";
        String valueSetPath = "../connectathon/fhir401/input/vocabulary/valueset";

        SparkSession spark = SparkSession.builder().config("spark.master", "local").appName("CQL on FHIR on Spark POC")
                .getOrCreate();

        FhirContext fhirContext = FhirContext.forCached(FhirVersionEnum.R4);
        IParser parser = fhirContext.newJsonParser();

        // Load CQL and set up CQL Translation
        List<String> cqlLibraries = readDirectoryIntoStrings(cqlPath);

        // Load all ValueSets into a Bundle
        DirectoryBundler bundler = new DirectoryBundler(fhirContext);
        Bundle terminologyBundle = (Bundle) bundler.bundle(valueSetPath);
        String valueSetBundleJson = parser.encodeResourceToString(terminologyBundle);

        // Load Resources
        // NOTE: This also wouldn't be how it's done in production since it's not
        // partitioned by patientId
        // and does not use any data-requirements. Works for a POC though.
        Dataset<String> dataBundleJson = createDataset(spark, fhirContext, bundler, parser, resourcePath);

        // Evaluate for each patient
        Dataset<String> results = dataBundleJson
                .mapPartitions(new EvaluatorMapPartitionsFunction(cqlLibraries, valueSetBundleJson), Encoders.STRING());

        // Write results
        results.write().text(outputPath);

        spark.stop();
    }

    private static Dataset<String> createDataset(SparkSession spark, FhirContext context, DirectoryBundler bundler,
            IParser parser, String resourcePath) {
        try {
            List<String> dataBundleJson = Files.list(Paths.get(resourcePath)).filter(x -> x.toFile().isDirectory())
                    .map(x -> bundler.bundle(x.toAbsolutePath().toString())).map(x -> parser.encodeResourceToString(x))
                    .collect(Collectors.toList());

            return spark.createDataset(dataBundleJson, Encoders.STRING());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String readAllBytesToString(Path filePath) {
        String content = "";

        try {
            content = new String(Files.readAllBytes(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return content;
    }

    private static List<String> readDirectoryIntoStrings(String filePath) {
        try {
            return Files.list(Paths.get(filePath)).filter(x -> x.toFile().getName().endsWith(".cql"))
                    .map(x -> readAllBytesToString(x)).filter(x -> x != null && !x.isEmpty())
                    .collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}