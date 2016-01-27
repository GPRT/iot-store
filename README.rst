IMPReSS IoTStore
================

This repository contains the REST server used in the IMPReSS project (Intelligent 
System Development Platform for Intelligent and Sustainable Society) Storage
Module. The IoTStore will connect to a running instance of OrientDB

Dependencies
------------
Packages you will need to install and deploy IoTStore:

* Java 8
* [Groovy 2.4](http://www.groovy-lang.org/download.html)
* [Grails 2.7](http://gradle.org/)
* [OrientDB 2.0.12](http://orientdb.com/download.php?email=unknown@unknown.com&file=orientdb-community-2.0.12.tar.gz&os=multi)

For Debian-based systems:

.. code-block:: bash

    $ sudo aptitude install openjdk-8-jdk openjdk-8-jre

With Java 8 installed, you may [install the sdkman](http://sdkman.io/install.html)
(former gvm, the Groovy enVironment Manager), to easily manage groovy
packages and its versions. With Sdkman, install groovy and gradle packages:

.. code-block:: bash

    $ sdk install groovy 2.4.4
    $ sdk install gradle 2.7

To run a OrientDB instance you can download and unpack the OrientDB 2.0.12, the
link is mentioned above. To run the local instance, go to OrientDBs folder:

.. code-block:: bash

    $ cd orientdb-community-2.0.12
    $ ./bin/server.sh

You should now see the following output:

.. code-block:: 

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
http://orientdb.com/docs/2.0/

Build and Run
-------------

.. _build_and_run_content_start:

Clone the repository

.. code-block:: bash

    $ git clone https://gitlab.com/impress/IoTStore

Use Gradle to build the dependencies for IoTStore:

.. code-block:: bash

    $ cd IoTStore/storage
    $ gradle build

With the dependencies built, since the IoTStore will connect to a running instance
of OrientDB, a local IP address of the instance must be specified. The default port
is 4567:

.. code-block:: bash
    
    $ gradle run -DiotStore.url=http://127.0.0.1:4567

The output should look like this:

.. code-block:: bash

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

*NOTE*: To use the linked data capabilities when using the direct IP address, use the
local network IP, not 127.0.0.1 as the example. Otherwise the you can't follow links
on requests responses.
    
.. _build_and_run_content_end:


License
-------

.. _license_content_start:

Copyright 2014 Lucas Lira Gomes x8lucas8x@gmail.com

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library. If not, see http://www.gnu.org/licenses/.

.. _license_content_end:
