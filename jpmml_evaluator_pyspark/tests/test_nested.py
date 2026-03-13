from jpmml_evaluator_pyspark import NestedPMMLTransformer
from jpmml_evaluator_pyspark.tests import IrisTest

class NestedPMMLTransformerTest(IrisTest):

	def _create_transformer(self, evaluator):
		return NestedPMMLTransformer(evaluator)

	def _get_success_col(self, transformer, pmml_df):
		return pmml_df[transformer.getResultsCol()]

	def checkParamDefaults(self, transformer):
		super(NestedPMMLTransformerTest, self).checkParamDefaults(transformer)
		self.assertEqual("pmmlResults", transformer.getResultsCol())

	def test_iris(self):
		self.checkIris()

	def test_iris_invalid(self):
		self.checkIrisInvalid()
