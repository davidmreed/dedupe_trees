from setuptools import setup

setup(
    name='dedupe_trees',
    version='1.0.0b3',
    description='A tool to compare and deduplicate divergent file trees, especially with different organization or hierarchy levels.',
    long_description=open('README.rst').read(),
    author='David Reed',
    author_email='david@ktema.org',
    url='https://github.com/davidmreed/dedupe_trees.py',
    license='MIT License',
    keywords=['deduplicate'],
    platforms='Any',
    classifiers=[
        'Development Status :: 3 - Beta',
        'Environment :: Console',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3'
    ],
    packages=['dedupe_trees'],
    entry_points={
        'console_scripts': [
            'dedupe_trees = dedupe_trees.__main__:main'
        ]
    },
)