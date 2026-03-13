from jpmml_evaluator_pyspark import _jvm
from pyspark.sql import SparkSession
from unittest import TestCase

import os

JPMML_EVALUATOR_SPARK_JARS = os.environ.get("JPMML_EVALUATOR_SPARK_JARS", "")
JPMML_EVALUATOR_SPARK_PACKAGES = os.environ.get("JPMML_EVALUATOR_SPARK_PACKAGES", "")

if JPMML_EVALUATOR_SPARK_JARS or JPMML_EVALUATOR_SPARK_PACKAGES:
	submit_args = []
	if JPMML_EVALUATOR_SPARK_JARS:
		submit_args.append("--jars {}".format(JPMML_EVALUATOR_SPARK_JARS))
	if JPMML_EVALUATOR_SPARK_PACKAGES:
		submit_args.append("--packages {}".format(JPMML_EVALUATOR_SPARK_PACKAGES))
	submit_args.append("pyspark-shell")

	os.environ['PYSPARK_SUBMIT_ARGS'] = " ".join(submit_args)

class PMMLTransformerTest(TestCase):

	@classmethod
	def setUpClass(cls):
		spark_builder = SparkSession.builder \
			.appName("PMMLTransformerTest") \
			.master("local[2]")

		cls.spark = spark_builder.getOrCreate()

		cls.sc = cls.spark.sparkContext

	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()

	def _load_evaluator(self, pmml_name):
		path = os.path.join(os.path.dirname(__file__), "resources", pmml_name)
		jvm = _jvm()
		pmml_is = jvm.java.io.FileInputStream(str(path))
		try:
			return jvm.org.jpmml.evaluator.LoadingModelEvaluatorBuilder() \
				.load(pmml_is) \
				.build()
		finally:
			pmml_is.close()

	def _load_dataframe(self, csv_name):
		path = os.path.join(os.path.dirname(__file__), "resources", csv_name)
		return self.spark.read \
			.option("header", "true") \
			.option("inferSchema", "true") \
			.csv(path)

class IrisTest(PMMLTransformerTest):

	def _create_transformer(self, evaluator):
		raise NotImplementedError()

	def _get_success_col(self, transformer, pmml_df):
		targetFields = transformer.evaluator.getTargetFields()
		if len(targetFields) != 1:
			raise ValueError()
		targetField = targetFields.get(0)
		return pmml_df[targetField.getName()]

	def _get_failure_col(self, transformer, pmml_df):
		return pmml_df[transformer.getExceptionCol()]

	def checkIris(self):
		evaluator = self._load_evaluator("DecisionTreeIris.pmml")
		transformer = self._create_transformer(evaluator)
		self.checkParamDefaults(transformer)

		df = self._load_dataframe("Iris.csv")
		self.checkIrisTransform(transformer, df, 150, 0)

	def checkIrisInvalid(self):
		evaluator = self._load_evaluator("DecisionTreeIris.pmml")
		transformer = self._create_transformer(evaluator)
		self.checkParamDefaults(transformer)

		df = self._load_dataframe("IrisInvalid.csv")
		self.checkIrisTransform(transformer, df, 147, 3)

	def checkParamDefaults(self, transformer):
		self.assertTrue(transformer.getInputs())
		self.assertTrue(transformer.getTargets())
		self.assertTrue(transformer.getOutputs())
		self.assertEqual("pmmlException", transformer.getExceptionCol())
		self.assertEqual("_target", transformer.getSyntheticTargetName())

	def checkIrisTransform(self, transformer, df, successCount, failureCount):
		pmml_df = transformer.transform(df)

		self.assertEqual(pmml_df.count(), successCount + failureCount)

		successCol = self._get_success_col(transformer, pmml_df)
		self.assertEqual(pmml_df.filter(successCol.isNotNull()).count(), successCount)
		self.assertEqual(pmml_df.filter(successCol.isNull()).count(), failureCount)

		failureCol = self._get_failure_col(transformer, pmml_df)
		self.assertEqual(pmml_df.filter(failureCol.isNotNull()).count(), failureCount)
		self.assertEqual(pmml_df.filter(failureCol.isNull()).count(), successCount)
