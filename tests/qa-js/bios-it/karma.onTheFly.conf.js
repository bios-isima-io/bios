// Karma configuration
module.exports = function (config) {
  var fs = require('fs');
  config.set({

    // base path that will be used to resolve all patterns (eg. files, exclude)
    basePath: '',

    // frameworks to use
    // available frameworks: https://npmjs.org/browse/keyword/karma-adapter
    frameworks: ['jasmine'],

    // list of files / patterns to load in the browser
    files: ['*.onTheFlyTest.js'],

    // list of files to exclude
    exclude: [],

    // preprocess matching files before serving them to the browser
    preprocessors: {
      '*.onTheFlyTest.js': ['webpack', 'env'],
    },

    protocol: 'https',

    httpsServerOptions: {
      key: fs.readFileSync('server.key.pem', 'utf8'),
      cert: fs.readFileSync('server.cert.pem', 'utf8'),
    },

    // test results reporter to use
    reporters: [/*'progress',*/ 'spec'],

    // setting up webpack configuration
    webpack: require('./webpack.test.config'),

    // web server port
    port: 9876,

    // enable / disable colors in the output (reporters and logs)
    colors: true,

    // level of logging
    // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
    logLevel: config.LOG_INFO,

    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: true,

    // start these browsers
    browsers: ['ChromeHeadless'],

    browserNoActivityTimeout: 1200000,

    // if true, Karma captures browsers, runs the tests and exits
    singleRun: true,

    // Concurrency level how many browser should be started simultaneous
    concurrency: Infinity,

    client: {
      captureConsole: true,
    },

    customLaunchers: {
      ChromeHeadless: {
        base: 'Chrome',
        flags: [
          // '--no-sandbox',
          '--headless',
          '--disable-gpu',
          '--remote-debugging-port=9222',
          '--ignore-certificate-errors',
        ],
      },
    },

    // karma-env-preprocessor configuration
    envPreprocessor: [
      'BIOS_DEBUG',
      'BIOS_ENDPOINT_JS',
      'TFOS_ENDPOINT',
      'TIMESTAMP_ON_THE_FLY_TEST',
      'TIMESTAMP_ON_THE_FLY_NO_FEATURES',
    ],

    // karma-spec-reporter configuration
    specReporter: {
      maxLogLines: 100,            // limit number of lines logged per test
      suppressErrorSummary: false, // do not print error summary
      suppressFailed: false,       // do not print information about failed tests
      suppressPassed: false,       // do not print information about passed tests
      suppressSkipped: true,       // do not print information about skipped tests
      showSpecTiming: true,        // print the time elapsed for each spec
      failFast: false              // test would finish with error when a first fail occurs.
    },

    // karma-junit-reporter configuration
    junitReporter: {
      outputDir: '../target/failsafe-reports', // results will be saved as $outputDir/$browserName.xml
      outputFile: 'qa-tests.xml', // if included, results will be saved as $outputDir/$browserName/$outputFile
      suite: '', // suite will become the package name attribute in xml testsuite element
      useBrowserName: true, // add browser name to report and classes names
      nameFormatter: undefined, // function (browser, result) to customize the name attribute in xml testcase element
      classNameFormatter: undefined, // function (browser, result) to customize the classname attribute in xml testcase element
      properties: {}, // key value pair of properties to add to the <properties> section of the report
      xmlVersion: null // use '1' if reporting to be per SonarQube 6.2 XML format
    }
  });
};
