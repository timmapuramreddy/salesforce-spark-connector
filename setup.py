from setuptools import setup, find_packages

setup(
    name="salesforce_connector",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "simple-salesforce>=1.12.4",
        "requests-oauthlib>=1.3.1",
        "pyspark>=3.0.0",  # Optional for Spark functionality
        "pandas>=1.3.0",   # Added for DataFrame conversions
        "duckdb>=0.9.0",   # Optional for DuckDB functionality
        "PyJWT>=2.4.0",
        "cryptography>=3.4.7",  # For JWT encryption
        "boto3>=1.26.0",    # For AWS functionality
        "pyarrow>=14.0.1",  # For Arrow conversions
    ],
    extras_require={
        'spark': ["pyspark>=3.0.0"],
        'duckdb': ["duckdb>=0.9.0"],
        'aws': ["boto3>=1.26.0"],
        'all': [
            "pyspark>=3.0.0",
            "duckdb>=0.9.0",
            "boto3>=1.26.0",
        ]
    },
    python_requires=">=3.8",
    author="Your Name",
    author_email="your.email@example.com",
    description="A scalable Salesforce connector with support for Spark and DuckDB",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/salesforce_connector",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
) 