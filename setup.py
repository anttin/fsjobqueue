import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fsjobqueue",
    version="0.0.1",
    author="anttin",
    author_email="muut.py@antion.fi",
    description="A simple filesystem queue mechanism",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/anttin/fsjobqueue",
    packages=setuptools.find_packages(),
    install_requires=[
      'python-dateutil',
      'watchdog'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
