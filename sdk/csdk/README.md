## Build Instruction

Run `./build.sh` to build the C-SDK library.
Do use this script for building for the first time as it sets up prerequisites and configuration.  Running build tools such as `make` and `ctest` is fine after the first successful build by `build.sh`.

There are several environment variables to configure the build outside the `build.sh` script:

`BUILD_DIR` (default: target):
Build directory name under the source code path. This must be a relative path from the project root
directory `${ROOT}/csdk-main`.

`BUILD_TYPE` (default: Release):
Specifies CMAKE build types. Possible values are `Debug`, `Release`, `RelWithDebInfo`, and
`MinSizeRel`.

The build script builds third party libraries under directory `${ROOT}/third-party` if
they are missing.  In order to rebuild the third party libraries, delete the directory
`${ROOT}/third-party` and run `build.sh` again.

## Running Unit Test
### Functional Testing
Run
```bash
make -C "${ROOT}/csdk-main/target test"
```
or
```bash
ctest --build-run-dir="${ROOT}/csdk-main/target/test"
```
or even simpler,
```bash
cd "${ROOT}/csdk-main/target" && ctest
```

### Checking for Memory Errors
In order to run the test cases under Valgrind, run
```bash
ctest -T memcheck
```
