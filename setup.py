from setuptools import setup

from easy_job import __version__

setup(
    name='easy_job',
    version=__version__,
    author='xiejiang',
    author_email='15201750637@163.com',
    description='Make Analysis Job Easy!',
    license='MIT',
    keywords='shell mysql',
    py_modules=['easy_job'],
    long_description=open('README.rst').read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3.5',
        'License :: OSI Approved :: MIT License',
    ]
)
