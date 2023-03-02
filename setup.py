import glob
import os

from setuptools import setup
from setuptools.extension import Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    cython_installed = False
    warnings.warn('Cython not installed, using pre-generated C source file.')
else:
    cython_installed = True


if cython_installed:
    python_source = 'lsm.pyx'
else:
    python_source = 'lsm.c'
    cythonize = lambda obj: obj

library_source = glob.glob('src/*.c')
lsm_extension = Extension(
    'lsm',
    sources=[python_source] + library_source)

setup(
    name='lsm-db',
    version='0.7.0',
    description='Python bindings for the SQLite4 LSM database.',
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize([lsm_extension]),
)
