from setuptools import setup, find_packages

setup(
    name="haystack-ai-cassandra",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "haystack-ai>=2.13.2",         
        "cassandra-driver>=3.29.2",
        "pydantic>=2.11.7"
    ],
    author="Gleb Ivanov",
    author_email="rvn17cj@gmail.com",
    description="Cassandra integration for Haystack AI with ANN vector search via SAI (Cassandra 5.0+)",
    url="https://github.com/pmd-gpt/cassandra-haystack",  
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License"
    ],
    python_requires='>=3.11.9',
)
