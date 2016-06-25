IMPReSS IoTStore
================

This repository contains the REST server used in the IMPReSS project (Intelligent 
System Development Platform for Intelligent and Sustainable Society) Storage
Module.

Dependencies
------------
Packages you will need to install and deploy IoTStore:

* Java 8
* Groovy 2.4: http://www.groovy-lang.org/download.html
* Gradle 2.7: http://gradle.org/
* OrientDB 2.1.16: http://orientdb.com/download.php?email=unknown@unknown.com&file=orientdb-community-2.1.16.tar.gz&os=multi

For Debian-based systems:

.. code-block:: bash

    $ sudo aptitude install openjdk-8-jdk openjdk-8-jre

With Java 8 installed, you may [install the sdkman](http://sdkman.io/install.html)
(former gvm, the Groovy enVironment Manager), to easily manage groovy
packages and its versions. With Sdkman, install groovy and gradle packages:

.. code-block:: bash

    $ sdk install groovy 2.4.6
    $ sdk install gradle 2.7

To run a OrientDB instance you can download and unpack the OrientDB 2.1.16, the
link is mentioned above. To run the local instance, go to OrientDBs folder:

.. code-block:: bash

    $ cd orientdb-community-2.1.16
    $ ./bin/server.sh

You should now see the following output:

.. code-block:: java

           .
          .`        `
          ,      `:.
         `,`    ,:`
         .,.   :,,
         .,,  ,,,
    .    .,.:::::  ````
    ,`   .::,,,,::.,,,,,,`;;                      .:
    `,.  ::,,,,,,,:.,,.`  `                       .:
     ,,:,:,,,,,,,,::.   `        `         ``     .:
      ,,:.,,,,,,,,,: `::, ,,   ::,::`   : :,::`  ::::
       ,:,,,,,,,,,,::,:   ,,  :.    :   ::    :   .:
        :,,,,,,,,,,:,::   ,,  :      :  :     :   .:
  `     :,,,,,,,,,,:,::,  ,, .::::::::  :     :   .:
  `,...,,:,,,,,,,,,: .:,. ,, ,,         :     :   .:
    .,,,,::,,,,,,,:  `: , ,,  :     `   :     :   .:
      ...,::,,,,::.. `:  .,,  :,    :   :     :   .:
           ,::::,,,. `:   ,,   :::::    :     :   .:
           ,,:` `,,.
          ,,,    .,`
         ,,.     `,                      S E R V E R
       ``        `.
                 ``
                 `

    2012-12-28 01:25:46:319 INFO Loading configuration from: config/orientdb-server-config.xml... [OServerConfigurationLoaderXml]
    2012-12-28 01:25:46:625 INFO OrientDB Server v1.6 is starting up... [OServer]
    2012-12-28 01:25:47:142 INFO -> Loaded memory database 'temp' [OServer]
    2012-12-28 01:25:47:289 INFO Listening binary connections on 0.0.0.0:2424 [OServerNetworkListener]
    2012-12-28 01:25:47:290 INFO Listening http connections on 0.0.0.0:2480 [OServerNetworkListener]
    2012-12-28 01:25:47:317 INFO OrientDB Server v1.6 is active. [OServer]


For further information on the configuration and usage or OrientDB, please see
http://orientdb.com/docs/2.1/


Build Dependencies
------------------

.. _build_content_start:

Clone the repository

.. code-block:: bash

    $ git clone https://gitlab.com/impress/IoTStore

Use Gradle to build the dependencies for IoTStore:

.. code-block:: bash

    $ cd IoTStore/storage
    $ gradle build


.. _build_content_end:

Create Schema and Enable Multitenancy
-------------------------------------

As many NoSQL solutions, OrientDB is schemaless. But for optimizing performance
IoTStore provides creation of indexes for the main classes in the IMPReSS' domain.
IoTStore implements Gradle tasks for creating and dropping the "iot" database. This
instance contains the model for optimal performance of IoTStore REST API.

For creating an "iot" database, enter the storage folder, which contains the
build.gradle file:

.. code-block:: bash

    $ cd IoTStore/storage
    $ gradle -q createdb

This may take a while. After that, proceed to buil and ultimately run IoTStore.
If for any reason you want to drop the database use the dropdb task:

.. code-block:: bash

    $ gradle -q dropdb

This will remove the "iot" database from local OrientDB instance. Remember, the
IoTStore will connect to a local OrientDB instance in a "iot" database.

Aggregation Mechanisms
----------------------
The IMPReSS Storage Module implements a hierarchical tree for organizing and
aggregating measurements in minutes, hours, days, months and years. This aggregation
is made through with the help of Java Hooks. These hooks work as triggers, loaded
when the OrientDB server is started. To deploy these hooks enter the folder:

.. code-block:: bash

    $ cd IoTStore/hooks

Build the package, generating a hooks-{version}.jar file located in IoTStore/hooks/build/libs.
This jar has to be located in the lib folder of OrientDB, so, when started, OrientDB
can load aggregation hooks.

.. code-block:: bash

    $ gradle build
    $ cp build/libs/hooks-{version}.jar {orientDb_path}/lib

To indicate OrientDB that this hook is to be loaded, add this block to the config file
located in {orientdb_directory}/config/orientdb-server-config.xml:

.. code-block:: xml
    
    [...]
            <entry name="cache.size" value="10000" />
            <entry name="storage.keepOpen" value="true" />
        </properties>
        <!-- ADD THIS BLOCK HERE -->
        <hooks>
            <hook class="org.impress.storage.MeasurementAggregator" position="REGULAR"/>
            <hook class="org.impress.storage.DeviceInitializer" position="REGULAR"/>
            <hook class="org.impress.storage.MeasurementRemover" position="REGULAR"/>
            <hook class="org.impress.storage.SampleCompressor" position="REGULAR"/>
        </hooks>
        <!-- /////////////////// -->
    </orient-server>

Then proceed with initializing OrientDB as in the Dependencies section.

Run
---

.. _run_content_start:

With the dependencies built, since the IoTStore will connect to a running instance
of OrientDB, a local IP address of the instance must be specified. The default port
is 4567:

.. code-block:: bash
    
    $ gradle run -DiotStore.url=http://127.0.0.1:4567

The output should look like this:

.. code-block:: 

    :storage:compilUP-TO-DATE
    :storage:compileGroovy UP-TO-DATE
    :storage:processResources UP-TO-DATE
    :storage:classes UP-TO-DATE
    :storage:run
    Jan 27, 2016 6:31:57 PM com.orientechnologies.common.log.OLogManager log
    INFO: OrientDB auto-config DISKCACHE=4,117MB (heap=1,762MB os=7,927MB disk=82,075MB)
    [Thread-3] INFO spark.webserver.SparkServer - == Spark has ignited ...
    [Thread-3] INFO spark.webserver.SparkServer - >> Listening on 0.0.0.0:4567
    [Thread-3] INFO org.eclipse.jetty.server.Server - jetty-9.0.2.v20130417
    [Thread-3] INFO org.eclipse.jetty.server.ServerConnector - Started ServerConnector@1d1361be{HTTP/1.1}{0.0.0.0:4567}
    > Building 80% > :storage:run

*NOTE*: To use linked data capabilities when using the direct IP address, use the
local network IP, not 127.0.0.1 as the example. Otherwise the you can't follow links
on requests responses.

.. _run_content_end:
