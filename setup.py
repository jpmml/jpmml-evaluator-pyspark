from setuptools import setup, find_packages

exec(open("jpmml_evaluator_pyspark/metadata.py").read())

setup(
	name = "jpmml_evaluator_pyspark",
	version = __version__,
	description = "PMML evaluator library for PySpark",
	author = "Villu Ruusmann",
	author_email = "villu.ruusmann@gmail.com",
	url = "https://github.com/jpmml/jpmml-evaluator-pyspark",
	download_url = "https://github.com/jpmml/jpmml-evaluator-pyspark/archive/" + __version__ + ".tar.gz",
	license = __license__,
	classifiers = [
		"Development Status :: 5 - Production/Stable",
		"Operating System :: OS Independent",
		"Programming Language :: Python",
		"Intended Audience :: Developers",
		"Intended Audience :: Science/Research",
		"Topic :: Software Development",
		"Topic :: Scientific/Engineering"
	],
	packages = find_packages(exclude = ["*.tests.*", "*.tests"]),
	exclude_package_data = {
		"" : ["README.md"],
	},
	python_requires = ">=3.8",
	install_requires = [
		"py4j",
		"pyspark"
	]
)
