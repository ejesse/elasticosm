from setuptools import setup, find_packages

long_description = open('README.md').read()

setup(
    name='elasticosm',
    version="0.1",
    description='ORM for Elasticsearch',
    long_description=long_description,
    author='Jesse Emery',
    author_email='jesse@jesseemery.com',
    url='https://github.com/ejesse/elasticosm',
    packages=find_packages(),
    license='BSD License',
    platforms=["any"],
)