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
        "numpy==1.22.0",
        "pandas==0.23.4",
        "pymysql==0.9.3",
        "sqlalchemy==1.3.3",
        "python-dotenv==0.8.2"
    ]
)
