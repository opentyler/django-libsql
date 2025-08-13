from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="django-libsql",
    version="0.1.4",
    description="LibSQL / Turso database backend for Django",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/opentyler/django-libsql",
    project_urls={
        "Homepage": "https://github.com/opentyler/django-libsql",
    },
    author="OpenTyler (fork maintainer); original by Aaron Kazah",
    python_requires=">=3.8",
    packages=find_packages(
        include=["django_libsql", "django_libsql.*"],
        exclude=["testapp", "testapp.*", "scripts", "scripts.*", "tests", "tests.*"],
    ),
    include_package_data=True,
    install_requires=[
        "Django>=3.0", # Compatible with python 3.8+
        "libsql>=0.1.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Framework :: Django",
    ],
    keywords=["django", "libsql", "turso", "sqlite", "database", "backend"],
)