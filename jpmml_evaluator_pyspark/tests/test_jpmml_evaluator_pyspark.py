from jpmml_evaluator_pyspark import classpath
from unittest import TestCase

class ConfigurationTest(TestCase):

	def testClasspath(self):
		spark3_jars = classpath("3.X")
		spark4_jars = classpath("4.X")

		self.assertEqual(1 + 15, len(spark3_jars))
		self.assertEqual(1 + 15, len(spark4_jars))
		self.assertNotEqual(spark3_jars[0], spark4_jars[0])
		self.assertEqual(set(spark3_jars[1:]), set(spark4_jars[1:]))
