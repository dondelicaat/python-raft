from setuptools import setup, find_packages

setup(
   name='raft',
   packages=find_packages('src'),
   package_dir={'': 'src'}
)