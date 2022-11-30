[![New Relic Experimental header](https://github.com/newrelic/opensource-website/raw/master/src/images/categories/Experimental.png)](https://opensource.newrelic.com/oss-category/#new-relic-experimental)

![GitHub forks](https://img.shields.io/github/forks/newrelic-experimental/newrelic-java-voltdb?style=social)
![GitHub stars](https://img.shields.io/github/stars/newrelic-experimental/newrelic-java-voltdb?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/newrelic-experimental/newrelic-java-voltdb?style=social)

![GitHub all releases](https://img.shields.io/github/downloads/newrelic-experimental/newrelic-java-voltdb/total)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/newrelic-experimental/newrelic-java-voltdb)
![GitHub last commit](https://img.shields.io/github/last-commit/newrelic-experimental/newrelic-java-voltdb)
![GitHub Release Date](https://img.shields.io/github/release-date/newrelic-experimental/newrelic-java-voltdb)


![GitHub issues](https://img.shields.io/github/issues/newrelic-experimental/newrelic-java-voltdb)
![GitHub issues closed](https://img.shields.io/github/issues-closed/newrelic-experimental/newrelic-java-voltdb)
![GitHub pull requests](https://img.shields.io/github/issues-pr/newrelic-experimental/newrelic-java-voltdb)
![GitHub pull requests closed](https://img.shields.io/github/issues-pr-closed/newrelic-experimental/newrelic-java-voltdb)
  
# New Relic Java Instrumentation for VoltDB

Provides instrumentation for VoltDB.  Client calls to VoltDB will show up as database calls.

## Installation

To install the instrumentation:
1. Download the latest release.
2. In the New Relic Java Agent directory, create a directory named extensions if it does not already exist.
3. Copy the downloaded jar files into the extensions directory
4. Restart the instance using VoltDB.  

## Building

If you make changes to the instrumentation code and need to build the instrumentation jars, follow these steps
1. Set environment variable NEW_RELIC_EXTENSIONS_DIR.  Its value should be the directory where you want to build the jars (i.e. the extensions directory of the Java Agent).   
2. Build one or all of the jars.   
  a. To build one jar, run the command:  gradlew _moduleName_:clean  _moduleName_:install    
  b. To build all jars, run the command: gradlew clean install

## Support

New Relic hosts and moderates an online forum where customers can interact with New Relic employees as well as other customers to get help and share best practices. Like all official New Relic open source projects, there's a related Community topic in the New Relic Explorers Hub. You can find this project's topic/threads here:

## Contributing
New Relic has open-sourced this project. This project is provided AS-IS WITHOUT WARRANTY OR DEDICATED SUPPORT. Issues and contributions should be reported to the project here on GitHub.

We encourage you to bring your experiences and questions to the [Explorers Hub](https://discuss.newrelic.com) where our community members collaborate on solutions and new ideas.

**A note about vulnerabilities**

As noted in our [security policy](../../security/policy), New Relic is committed to the privacy and security of our customers and their data. We believe that providing coordinated disclosure by security researchers and engaging with the security community are important means to achieve our security goals.

If you believe you have found a security vulnerability in this project or any of New Relic's products or websites, we welcome and greatly appreciate you reporting it to New Relic through [HackerOne](https://hackerone.com/newrelic).

## License
Instrumentation for VoltDB is licensed under the [Apache 2.0](http://apache.org/licenses/LICENSE-2.0.txt) License.
