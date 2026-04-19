from jpmml_evaluator_pyspark import spark_jars
from jpmml_evaluator_pyspark.wrapper import _jvm
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from tempfile import TemporaryDirectory
from unittest import TestCase

import os

def _clone(obj):
	with TemporaryDirectory() as tmpDir:
		obj.write() \
			.overwrite() \
			.save(tmpDir)

		cloned_obj = type(obj) \
			.load(tmpDir)

		return cloned_obj

class PMMLTransformerTest(TestCase):

	@classmethod
	def setUpClass(cls):
		cls.spark = SparkSession.builder \
			.appName("PMMLTransformerTest") \
			.master("local[2]") \
			.config("spark.jars", spark_jars()) \
			.getOrCreate()

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

		# Direct persistence
		transformer = _clone(transformer)
		self.checkParamDefaults(transformer)

		df = self._load_dataframe("Iris.csv")
		self.checkIrisTransform(transformer, df, 150, 0)

	def checkIrisInvalid(self):
		evaluator = self._load_evaluator("DecisionTreeIris.pmml")
		transformer = self._create_transformer(evaluator)
		self.checkParamDefaults(transformer)

		# Pipeline-mediated persistence
		pipelineModel = PipelineModel(stages = [transformer])
		pipelineModel = _clone(pipelineModel)

		transformer = pipelineModel.stages[0]
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
