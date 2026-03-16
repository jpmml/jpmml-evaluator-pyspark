from py4j.java_gateway import JavaObject
from pyspark import SparkContext
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import JavaMLWritable, MLReadable, MLReader
from pyspark.ml.wrapper import JavaTransformer

import sys
import types

def _jvm():
	return SparkContext._jvm

def _ensure_module(module_path):
	segments = module_path.split(".")
	for i in range(len(segments)):
		path = ".".join(segments[:i + 1])
		if path not in sys.modules:
			sys.modules[path] = types.ModuleType(path)
		if i > 0:
			parent_path = ".".join(segments[:i])
			setattr(sys.modules[parent_path], segments[i], sys.modules[path])
	return sys.modules[module_path]

def _register_jpmml_class(py_class):
	java_class_name = py_class._java_class_name
	parts = java_class_name.rsplit(".", 1)
	module = _ensure_module(parts[0])
	if not hasattr(module, parts[1]):
		setattr(module, parts[1], py_class)

def _create_java_object(java_class_name, *args):
	return getattr(_jvm(), java_class_name)(*args)

def _create_java_transformer(java_class_name, evaluator):
	if isinstance(evaluator, JavaObject):
		return _create_java_object(java_class_name, evaluator)
	else:
		return None

class PMMLTransformer(JavaTransformer, JavaMLWritable, MLReadable):

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

	@classmethod
	def read(cls):
		return _JavaReader(cls, cls._java_class_name)

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

class _JavaReader(MLReader):

	def __init__(self, py_class, java_class_name):
		super().__init__()
		self.py_class = py_class
		self.java_class_name = java_class_name

	def load(self, path):
		java_obj = getattr(_jvm(), self.java_class_name).load(path)
		
		py_obj = self.py_class.__new__(self.py_class)
		PMMLTransformer.__init__(py_obj, java_obj = java_obj)
		
		return py_obj

_register_jpmml_class(FlatPMMLTransformer)
_register_jpmml_class(NestedPMMLTransformer)