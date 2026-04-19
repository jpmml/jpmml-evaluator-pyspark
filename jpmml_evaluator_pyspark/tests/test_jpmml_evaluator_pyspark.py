from jpmml_evaluator_pyspark import spark_jars
from unittest import TestCase

class ConfigurationTest(TestCase):

	def testJars(self):
		spark3_jars = spark_jars("3.X").split(",")
		spark4_jars = spark_jars("4.X").split(",")

		self.assertEqual(1 + 15, len(spark3_jars))
		self.assertEqual(1 + 15, len(spark4_jars))
		self.assertNotEqual(spark3_jars[0], spark4_jars[0])
		self.assertEqual(set(spark3_jars[1:]), set(spark4_jars[1:]))
