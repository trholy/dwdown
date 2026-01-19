from setuptools import setup, find_packages

setup(
    name="dwdown",
    version="0.2.0",
    author="Thomas R. Holy",
    author_email="thomas.robert.holy@gmail.com",
    description="Download weather forecasts and historical data from DWD",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/trholy/dwdown",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    license_files=('LICENSE',),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=[
        "lxml>=5.3.0",
        "minio>=7.2.15",
        "pandas>=2.2.3",
        "requests>=2.32.4",
        "xarray>=2024.7.0",
        "urllib3>=2.3.0",
        "cfgrib>=0.9.15.0"
    ],
    extras_require={
        "test": [
            "pytest>=7.2",
            "ruff>= 0.9.6",
            "chardet>=5.2.0",
            "responses>=0.25.0",
        ]
    },
    test_suite='pytest',
)
