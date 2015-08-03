import os

from distutils.core import setup, Extension
from Cython.Build import cythonize


python_source = 'lsm.pyx'
library_source = 'src/sqlite4.c'

lsm_extension = Extension(
    'lsm',
    sources=[python_source, library_source])

setup(
    name='lsm-db',
    version='0.1.0',
    description='Python bindings for the SQLite4 LSM database.',
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize(lsm_extension),
)
