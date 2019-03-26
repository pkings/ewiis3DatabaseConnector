from setuptools import find_packages, setup

setup(
    name="ewiis3DatabaseConnector",
    version="0.0.1",
    description="Connects to the ewiis3 broker's database scheme",
    author="Peter Kings",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "Click>=7.0",
        "numpy>=1.16.1",
        "pandas>=0.23.4"
    ],
    entry_points="""
            [console_scripts]
            evsim=ewiis3DatabaseConnector.ewiis3DatabaseConnector:cli
        """,
)
