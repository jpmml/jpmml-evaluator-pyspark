JPMML-Evaluator-PySpark [![Build Status](https://github.com/jpmml/jpmml-evaluator-pyspark/workflows/pytest/badge.svg)](https://github.com/jpmml/jpmml-evaluator-pyspark/actions?query=workflow%3A%22pytest%22)
=======================

PMML evaluator library for PySpark.

# Features #

This package is a thin Python wrapper around the [JPMML-Evaluator-Spark](https://github.com/jpmml/jpmml-evaluator-spark) library.

# Prerequisites #

* PySpark 3.0.X through 3.5.X, 4.0.X or 4.1.X.
* Python 3.8 or newer.

# Installation #

Installing a release version from PyPI:

```bash
pip install jpmml_evaluator_pyspark
```

Alternatively, installing the latest snapshot version from GitHub:

```bash
pip install --upgrade git+https://github.com/jpmml/jpmml-evaluator-pyspark.git
```

# Configuration #

JPMML-Evaluator-PySpark must be paired with JPMML-Evaluator-Spark based on the following compatibility matrix:

| PySpark version | JPMML-Evaluator-Spark branch | Latest JPMML-Evaluator-Spark version |
|-----------------|------------------------------|--------------------------------------|
| 4.0.X and 4.1.X | [`master`](https://github.com/jpmml/jpmml-evaluator-spark/tree/master) | 2.1.2 |
| 3.0.X through 3.5.X | [`2.0.X`](https://github.com/jpmml/jpmml-evaluator-spark/tree/2.0.X) | 2.0.2 |

## Local setup

JPMML-Evaluator-PySpark version 0.3.0 and newer bundle JPMML-Evaluator-Spark JAR files for quick programmatic setup.

Use the `jpmml_evaluator_pyspark.spark_jars()` utility function to obtain a PySpark-version dependent classpath string, and pass it as `spark.jars` configuration entry when building a Spark session:

```python
from pyspark.sql import SparkSession

import jpmml_evaluator_pyspark

spark = SparkSession.builder \
	.config("spark.jars", jpmml_evaluator_pyspark.spark_jars()) \
	.getOrCreate()
```

## Cluster setup

Use the `jpmml_evaluator_pyspark.spark_jars_packages()` utility function to obtain a PySpark-version dependent Apache Maven package coordinates string:

```python
import jpmml_evaluator_pyspark

print(jpmml_evaluator_pyspark.spark_jars_packages())
```

Pass this value to `pyspark` or `spark-submit` using the `--packages` command-line option:

```bash
$SPARK_HOME/bin/pyspark --packages $(python -c "import jpmml_evaluator_pyspark; print(jpmml_evaluator_pyspark.spark_jars_packages())")
```

# Usage #

The "heart" of the PMML transformer is an `org.jpmml.evaluator.Evaluator` object.

Grab a JVM handle, and build the evaluator object from a streamable PMML resource using the `org.jpmml.evaluator.LoadingModelEvaluatorBuilder` builder class as usual.

Building from a PMML file:

```python
from jpmml_evaluator_pyspark import _jvm

jvm = _jvm()

pmmlIs = jvm.java.io.FileInputStream("/path/to/DecisionTreeIris.pmml")
try:
	evaluator = jvm.org.jpmml.evaluator.LoadingModelEvaluatorBuilder() \
		.load(pmmlIs) \
		.build()
finally:
	pmmlIs.close()

evaluator.verify()
```

JPMML-Evaluator-PySpark faithfully mirrors the [JPMML-Evaluator-Spark public API](https://github.com/jpmml/jpmml-evaluator-spark/?tab=readme-ov-file#public-api).
The only notable change is that the `org.jpmml.evaluator.spark` Java/Scala package name has been truncated to the `jpmml_evaluator_pyspark` Python module name.

Constructing a PMML transformer:

```python
from jpmml_evaluator_pyspark import FlatPMMLTransformer, NestedPMMLTransformer

pmmlTransformer = FlatPMMLTransformer(evaluator)
#pmmlTransformer = NestedPMMLTransformer(evaluator)
```

Transforming data using a PMML transformer:

```python
df = spark.read.csv("/path/to/Iris.csv", header = True, inferSchema = True)

pmmlDf = pmmlTransformer.transform(df)
pmmlDf.show()
```

# License #

JPMML-Evaluator-PySpark is licensed under the terms and conditions of the [GNU Affero General Public License, Version 3.0](https://www.gnu.org/licenses/agpl-3.0.html).
For a quick summary of your rights ("Can") and obligations ("Cannot" and "Must") under AGPLv3, please refer to [TLDRLegal](https://tldrlegal.com/license/gnu-affero-general-public-license-v3-(agpl-3.0)).

If you would like to use JPMML-Evaluator-PySpark in a proprietary software project, then it is possible to enter into a licensing agreement which makes it available under the terms and conditions of the [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause) instead.

# Additional information #

JPMML-Evaluator-PySpark is developed and maintained by Openscoring Ltd, Estonia.

Interested in using [Java PMML API](https://github.com/jpmml) software in your company? Please contact [info@openscoring.io](mailto:info@openscoring.io)
