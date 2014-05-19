#!/usr/bin/env python
from setuptools import setup, find_packages

pkg_root = 'src/python'

setup(name='bshotgun',
      version='0.1.0',
      description='Write better shotgun tools, faster.',
      author='Sebastian Thiel',
      author_email='byronimo@gmail.com',
      url='https://github.com/Byron/bshotgun',
      packages=find_packages(pkg_root),
      package_dir={'' : pkg_root},
      package_data={'bshotgun.tests' : ['fixtures/samples/scrambled-ds1/data.jsonz/*.json.z',
                                        'fixtures/samples/scrambled-ds1/schema.jsonz/*.pickle.zip']}
     )
