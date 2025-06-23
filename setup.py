"""
Setup script for Newsagger.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="newsagger",
    version="0.1.0",
    author="Newsagger Team",
    description="Library of Congress News Archive Aggregator",
    long_description="A tool for downloading and processing complete news archives from the Library of Congress Chronicling America collection.",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Researchers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "click>=8.1.0",
        "tqdm>=4.66.0",
    ],
    entry_points={
        "console_scripts": [
            "newsagger=newsagger.cli:cli",
        ],
    },
)