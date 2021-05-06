import setuptools

with open ("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tidemarks",
    version="0.0.2",
    author="Gifford Tompkins III",
    author_email="giffordtompkinsiii@gmail.com",
    description="Data Pipelines: Extraction, Transformation and Loading Procedures for The Tides Group.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/giffordtompkinsiii/lodestar",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'google-api-core>=1.22.0',
        'google-api-python-client>=1.10.0',
        'google-auth>=1.20.1',
        'google-auth-httplib2>=0.0.4',
        'google-auth-oauthlib>=0.4.1',
        'googleapis-common-protos>=1.52.0',
        'ibapi>=9.76.1',
        'ipywidgets>=7.5.1',
        'matplotlib>=3.2.1',
        'pandas>=1.1.4',
        'rsa>=4.1',
        'seaborn>=0.10.1',
        'SQLAlchemy>=1.3.18',
        'tqdm>=4.48.0',
        'pandas>=1.1.4',
        'psycopg2-binary>=2.8.5',
        'xlrd>=1.2.0',
        'yfinance>=0.1.54'
    ],
    python_requires='>=3.8.0'
)
