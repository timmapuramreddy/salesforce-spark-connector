from setuptools import setup, find_packages

setup(
    name="salesforce-spark-connector",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "simple-salesforce>=1.12.0",
        "pyspark>=3.0.0",
        "typing-extensions>=4.0.0",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A scalable Salesforce connector for Apache Spark",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/salesforce-spark-connector",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
) 