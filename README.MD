# CQL Evaluator on Spark

## Overview

This repository provides a proof of concept of the use of the [cql-evaluator](https://github.com/DBCG/cql-evaluator) on Spark project running against FHIR resources. The [connectathon](https://github.com/DBCG/connectathon) IG is used as the source for the data, valuesets, and CQL logic.

The following is performed:

1. Load CQL content into a String
1. Load ValueSet content as JSON into a String
1. Load clinical data into Bundles
1. Per patient, execute the CQL
1. Write the evaluation results

While this is only a proof-of-concept and is hard-coded, it demonstrates that executing Quality Measures against standard (i.e. JSON or XML) FHIR resources is possible.

## Usage

Maven, Java 8, and Spark need to be installed on the target machine.

The connectathon IG needs to be checked out into a sibling directory of this project:

```bash
/your/home/connectathon
/your/home/spark-cql-fhir
```

The project is currently hard-coded to run on a local Spark instance. The project can then be executed with:

```bash
mvn package
spark-submit --class org.opencds.cqf.cql.spark.sample.Main  --master local[8] ./target/spark-cql-fhir-1.0-SNAPSHOT.jar
```

The results of the EXM74 CQL will be written to the ```output``` folder.

A production implementation would require numerous changes, such as broadcasting non-patient compartment data (e.g. medications) across a cluster or shuffling patient compartment data (e.g. lab results) such that a given node is able to read the entire clinical history for a given patient, etc. There are also numerous considerations around data access and provenance that would have to be addressed.

There are also several enhancements that could be made to the upstream cql-engine and cql-evaluator project to facilitate easier usage in Spark, such as implementing Serializable interface for commonly used classes.

## Related Projects

[Clinical Quality Language](https://github.com/cqframework/clinical_quality_language) - Tooling in support of the CQL specification, including the CQL verifier/translator used in this project.

[CQL Engine](https://github.com/DBCG/cql_engine) - The open source Java-based ELM evaluation engine used in this project.

## License

Copyright 2019+ Dynamic Content Group, LLC (dba Alphora)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
