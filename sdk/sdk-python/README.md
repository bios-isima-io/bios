# TFOS SDK
This directory includes source code for packaging TFOS python SDK.

## Directory Structure

```
sdk/
|-- doc/   : Documentation
|-- tfos/  : TFOS SDK package
`-- test/  : SDK unit tests
```

## Documentation
### Docstrings Convention
Use Google Style Python Docstring:
http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html

### Doc Generator
We use [Sphinx](http://www.sphinx-doc.org/en/master/) to generate documentation with following plugins:

- autodoc (installed in default) -- to generate documentation from docstrings in source code
- [napoleon](https://pypi.python.org/pypi/sphinxcontrib-napoleon) -- to interpret Google Style Python Docstring.

### Building Documentation
```
cd ${ROOT}/sdk/doc
make
```

Then HTML documentation is generated under `$ROOT/target/sdk-doc/html`.

The documentation build is run by maven in compile phase.

### Building
```
cd ${ROOT}/sdk-python
python3 setup.py build
```

### Setting PYTHONPATH for development
```
_kernel=$(uname | awk '{print tolower($0)}')
_arch=$(uname -m)
_pyversion=$(python3 --version | awk '{print $2}' | sed -r 's/^([0-9]+\.[0-9]+)\..*/\1/g')
_pylib="${ROOT}/sdk-python/build/lib.${_kernel}-${_arch}-${_pyversion}"
export PYTHONPATH="${ROOT}/sdk-python:${PYTHONPATH}:${_pylib}"
```

### Packaging
```
cd ${ROOT}/sdk-python
python3 setup.py sdist bdist_wheel --dist-dir=${ROOT}/sdk-python/target
```

The package is created under directory `${ROOT}/sdk-python/target`.

In order to install the SDK, after untaring the product package,

```
tar xvf tfos-sdk-${BUILD_VERSION}.tar.gz
cd tfos-sdk-${BUILD_VERSION}
pip3 install -r requirements.txt .
```
