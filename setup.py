from __future__ import absolute_import
import setuptools
from setuptools import setup
import os
import sys
_here = os.path.abspath(os.path.dirname(__file__))


from setuptools.command.install import install
from subprocess import check_call

# class PostInstallCommand(install):
#     """Post-installation for installation mode."""
#     def run(self):
#         install.run(self)
#         check_call('lltk configure'.split())




with open("README.md", "r", encoding='utf-8', errors='ignore') as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding='utf-8', errors='ignore') as fh:
    requirements = [x.strip() for x in fh.read().split('\n') if x.strip()]

setup(
    name='lltk-dh',
    version='0.5.14',
    description=('Literary Language Toolkit (LLTK): corpora, models, and tools for the digital humanities'),
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Ryan Heuser',
    author_email='rj416@cam.ac.uk',
    url='https://github.com/quadrismegistus/lltk',
    license='MIT',
    packages=setuptools.find_packages(),
    install_requires=requirements,
    scripts=['bin/lltk'],
    include_package_data=True,
    classifiers=[
        #'Development Status :: 3 - Alpha',
        #'Intended Audience :: Science/Research',
        #'Programming Language :: Python :: 2.7',
        #'Programming Language :: Python :: 3.6'
    ],
    # cmdclass={
    #     'install': PostInstallCommand,
    # },

)
