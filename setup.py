from setuptools import setup, find_packages

setup(
    name="dwdown",
    version="0.1.0",
    author="Thomas R. Holy",
    author_email="thomas.robert.holy@gmail.com",
    description="Download weather forecasts from DWD",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://git.rz.uni-jena.de/to82lod/dwdown",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "lxml>=5.3.0",
        "minio>=7.2.15",
        "pandas>=2.2.3",
        "requests>=2.32.3",
        "xarray>=2024.7.0",
    ],
    extras_require={
        "dev": [
            "pytest",
            "ruff"
        ]
    },
    test_suite='pytest',
)
