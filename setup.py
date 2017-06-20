from setuptools import setup, find_packages

setup(
    name="sql_magic",
    version="0.0.2",
    author='Chris Rawles',
    author_email='crawles@pivotal.io',
    packages=find_packages(),
    install_requires=(
        'findspark',
        'ipython',
        'jupyter',
        'pandas',
        'sqlparse',
        'traitlets'
    ),
    tests_require=['pytest>=3.0'],
    include_package_data=True
)
