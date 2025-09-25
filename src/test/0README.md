To run these tests you should run mvn test.
If you need to debug rasterx tests then you need to use -DagentLib='${jdwp.agent}' in the mvn command.
jdwp.agent is already defined in the pom.xml.
This is required to be able to attach a debugger to the docker container running the tests.

Note:
If running with jdwp.agent param in mvn test, python3 defunct process can occur for rst_mapalgebra and rst_ndvi tests.
If you need to debug these tests that will work, the issue only occurs on draining stdout/err of the process.