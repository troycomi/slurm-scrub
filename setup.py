from setuptools import setup, find_packages

setup(
    name='slurm-scrub',
    version=0.1,
    author='Troy Comi',
    description='Get resource usage from sacct',
    url='https://github.com/troycomi/slurm-scrub',
    packages=find_packages(),
    install_requires=[
        'Click',
        'Pandas',
        'Numpy',
        'Scipy'
    ],
    entry_points='''
        [console_scripts]
        slurm-scrub=slurm_scrub.cli:main
''',
)
