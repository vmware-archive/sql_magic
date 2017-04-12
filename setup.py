from setuptools import setup, find_packages

setup(
    name="sql_magic",
    version="0.0.1",
    author='Chris Rawles',
    author_email='crawles@pivotal.io',
    packages=find_packages(),
    install_requires=(
        'ipython',
        'jupyter',
        'pandas',
        'traitlets'
    ),
    include_package_data=True
)
