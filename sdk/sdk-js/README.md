## Environment Variable for Jest

## Adding and Executing Unit Tests

- Test file name: `*.test.js`; place the files anywhere in the SDK directory (bios)
- To execute the tests, run `yarn test` or `yarn test:watch` in directory $ROOT/sdk-js/bios

## Adding and Executing Integration Tests

- Test file name convention: `*test.js`. Place test files ONLY IN THE TOP LEVEL of the IT directory
  (sdk-js/bios-it).
- Run bios server locally, listening on port 8443.
- Ensure you have node js version 20, e.g. 20.3.1 which is the version the UI is using.
  Ubuntu 22.04 comes with an earlier version of node, so we need to upgrade.
    ```
    sudo npm install -g n
    sudo n 20.3.1
    ```
    Restart all terminal sessions for changes to take effect. Run `node --version` to verify.
- Run `./it.sh` in the directory `${ROOT}/sdk-js`.
    - The script `it.sh` configures the IT environment.
- After running `it.sh` once, you can repeat the tests by running `./it-execute.sh`.
    - The watch mode is available, too. Run `./it-execute.sh test:watch`.
- Run `./devsetup.sh` in the directory `${ROOT}/sdk-js`.
  - This sets up a symbolic link from `sdk-js/bios-it/node_modules/bios-sdk` to `sdk-js/bios`,
    which is where your JS SDK gets built. This allows the IT to pick up the latest JS SDK built
    from your changes, rather than the bios-sdk dependency pulled from NPM repository.
- If you get errors such as `Network Error` or `Not logged in; Login to start the session`, some
  access control headers may not be set in your Wildfly environment. Apply the patch file
  standalone.xml.for-dev-vm-js-integration-tests.patch to your
  $WILDFLY_HOME/standalone/configuration/standalone.xml file.

## Debugging Integration Tests
If you want to run an individual test or test suite, there is no convenient configuration or
command line argument to achieve it. You will need to temporarily change the integration test's
source code.
1. Select or ignore a specific test / test suite:
    - To run a single test suite: Add an `f` in front of `describe(` to make it `fdescribe(`.
    - To run a single test: Add an `f` in front of `it(` to make it `fit(`.
    - To exclude running a single test suite: Add an `x` in front of `describe(` to make it
       `xdescribe(`.
    - To exclude a single test: Add an `x` in front of `it(` to make it `xit(`.
1. Turn on debug logging:
    - Add BIOS_DEBUG to file ./test-env.sh and set its value to the list of debugLog items
        you want printed, separated by '-' characters,
        e.g. `export BIOS_DEBUG=getAttributeSynopsis-exceptions`
1. Run `./it-execute.sh test:debug`
1. **Remember to reset the change in #1 above before checking in your code!**

## Style Check
```
cd bios
yarn lint
```

## Helper scripts in this directory

### build.sh
This script builds the JS SDK. What this script does are:

- Generate the package.json file of the SDK from its template file package.bios.json.tmpl.
- Install dependency packages.
- Build Protocol Buffer static classes.
- Generate the package.json file of the integration test environment from the template
  package.it.json.templ.

### it.sh
Integration test automation script. As prerequisites, the script requires following environment
variables:

- BIOS_ENDPOINT_JS - Set URL for JS test program to connect. If omitted, TFOS_ENDPOINT is used
  instead.
- TFOS_ENDPOINT - Set URL for Python SDK (and JS SDK if BIOS_ENDPOINT_JS is omitted) endpoint.
- SSL_CERT_FILE - TLS certificates file path.
- PYTHONPATH - Set path to Python SDK; necessary for the setup program.

The script runs following steps:

- Creates test tenant and streams.
- Ingests several events
- Publish the current JS SDK module to the local repository http://localhost:4873
- Set up the integration test module via the local repository http://localhost:4873
- Sleep until the integration tests are ready to run (waits for the necessary rollup happen).
- Execute the test.

This script leaves test env configuration file `test-env.sh`. Once this file is created,
you can repeat the test execution without sleeping by calling the script `it-execute.sh`.

### it-execute.sh
Executes integration tests. There are several prerequisite environment variables for the test;
sourcing `test-env.sh` sets them.

There are several options for running test. They can be specified by specifying parameter
`test:<mode>` as an argument of `it-execute.sh`, such as `it-execute.sh test:debug`.

Follows are supported optional test modes:

- test:watch -- Run test in watch mode. In this mode, the test would be re-run when any of test
                cases are updated.
- test:report -- Run test in report mode. In this mode, Junit style report is created in directory
                 `target/failsafe-reports`.
- test:debug -- Run test in debug mode. In this mode, the test runs with a browser window, which is
                useful to investigate browser internals.

### devsetup.sh

After running `it.sh`, you can set up a dev/debug environment harnessed by the integration tests
by calling this script. This script replaces the JS SDK module in the integration test environment
(`bios-it/node_modules/bios`) by the symbolic link to the SDK source code directory. After that,
when you run the IT program by `./it-execute.sh test:watch`, the integration test env runs
the test when it detects a code change in the SDK source code tree.
