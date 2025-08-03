from setuptools import setup, find_packages

# This setup.py is kept for compatibility, but the main build configuration
# is now in pyproject.toml using maturin for the Rust extension

setup(
    name='mssql-python-rust',
    version='0.1.0',
    author='Timothy Bender',
    author_email='tbenderfcs@gmail.com',
    description='A high-performance Python library for Microsoft SQL Server using Rust and Tiberius',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(where='python'),
    package_dir={'': 'python'},
    install_requires=[
        # Core dependencies will be handled by pyproject.toml
    ],
    extras_require={
        'dev': [
            'pytest>=7.0',
            'pytest-asyncio>=0.21',
            'black>=23.0',
            'ruff>=0.1',
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Rust',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    python_requires='>=3.8',
    zip_safe=False,
)