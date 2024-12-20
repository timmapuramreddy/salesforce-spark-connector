from setuptools import setup, find_packages

extras_require = {
    'dev': [
        'pytest>=6.0.0',
        'pytest-cov>=2.0.0',
        'flake8>=3.9.0',
        'black>=21.0',
        'mypy>=0.900',
        'twine>=3.4.0',
        'build>=0.7.0',
    ],
}

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
    extras_require=extras_require,
) 