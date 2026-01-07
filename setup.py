import glob
import os
from setuptools import setup
from setuptools.extension import Extension
try:
    from Cython.Build import cythonize
    cython_installed = True
except ImportError:
    cython_installed = False

if cython_installed:
    python_source = 'lsm.pyx'
else:
    python_source = 'lsm.c'
    cythonize = lambda obj: obj

library_source = glob.glob('src/*.c')
lsm_extension = Extension(
    'lsm',
    sources=[python_source] + library_source)

setup(name='lsm-db', ext_modules=cythonize([lsm_extension]))
