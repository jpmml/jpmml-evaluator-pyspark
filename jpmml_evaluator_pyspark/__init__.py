from py4j.java_gateway import JavaObject
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.wrapper import JavaTransformer
from pyspark.sql import SparkSession

def _jvm():
	spark = SparkSession.getActiveSession()
	if spark is None:
		raise RuntimeError("Apache Spark session not found")
	return spark._jvm

def _create_java_object(java_class_name, *args):
	return getattr(_jvm(), java_class_name)(*args)

def _create_java_transformer(java_class_name, evaluator):
	if isinstance(evaluator, JavaObject):
		return _create_java_object(java_class_name, evaluator)
	else:
		return None

class PMMLTransformer(JavaTransformer, DefaultParamsReadable, DefaultParamsWritable):

	_java_class_name = "org.jpmml.evaluator.spark.PMMLTransformer"

	inputs = Param(Params._dummy(), "inputs", "Copy input columns", typeConverter = TypeConverters.toBoolean)
	targets = Param(Params._dummy(), "targets", "Produce columns for PMML target fields", typeConverter = TypeConverters.toBoolean)
	outputs = Param(Params._dummy(), "outputs", "Produce columns for PMML output fields", typeConverter = TypeConverters.toBoolean)
	exceptionCol = Param(Params._dummy(), "exceptionCol", "Exception column name", typeConverter = TypeConverters.toString)
	syntheticTargetName = Param(Params._dummy(), "syntheticTargetName", "Name for a synthetic target field", typeConverter = TypeConverters.toString)

	def __init__(self, java_obj = None):
		super(PMMLTransformer, self).__init__(java_obj = java_obj)
		if java_obj is not None:
			self._resetUid(java_obj.uid())
			self._transfer_params_from_java()

	@property
	def evaluator(self):
		return self._java_obj.evaluator()

	def getInputs(self):
		return self.getOrDefault(self.inputs)

	def setInputs(self, value):
		return self._set(inputs = value)

	def getTargets(self):
		return self.getOrDefault(self.targets)

	def setTargets(self, value):
		return self._set(targets = value)

	def getOutputs(self):
		return self.getOrDefault(self.outputs)

	def setOutputs(self, value):
		return self._set(outputs = value)

	def getExceptionCol(self):
		return self.getOrDefault(self.exceptionCol)

	def setExceptionCol(self, value):
		return self._set(exceptionCol = value)

	def getSyntheticTargetName(self):
		return self.getOrDefault(self.syntheticTargetName)

	def setSyntheticTargetName(self, value):
		return self._set(syntheticTargetName = value)

class FlatPMMLTransformer(PMMLTransformer):

	_java_class_name = "org.jpmml.evaluator.spark.FlatPMMLTransformer"

	def __init__(self, evaluator = None):
		java_obj = _create_java_transformer(FlatPMMLTransformer._java_class_name, evaluator)
		super(FlatPMMLTransformer, self).__init__(java_obj = java_obj)

class NestedPMMLTransformer(PMMLTransformer):

	_java_class_name = "org.jpmml.evaluator.spark.NestedPMMLTransformer"

	resultsCol = Param(Params._dummy(), "resultsCol", "Results column name", typeConverter = TypeConverters.toString)

	def __init__(self, evaluator = None):
		java_obj = _create_java_transformer(NestedPMMLTransformer._java_class_name, evaluator)
		super(NestedPMMLTransformer, self).__init__(java_obj = java_obj)

	def getResultsCol(self):
		return self.getOrDefault(self.resultsCol)

	def setResultsCol(self, value):
		return self._set(resultsCol = value)
