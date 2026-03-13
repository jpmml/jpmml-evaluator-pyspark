from jpmml_evaluator_pyspark import FlatPMMLTransformer
from jpmml_evaluator_pyspark.tests import IrisTest

class FlatPMMLTransformerTest(IrisTest):

	def _create_transformer(self, evaluator):
		return FlatPMMLTransformer(evaluator)

	def test_iris(self):
		self.checkIris()

	def test_iris_invalid(self):
		self.checkIrisInvalid()
