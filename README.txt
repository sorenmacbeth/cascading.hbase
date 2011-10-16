
Welcome

 This is the Cascading.HBase module.

 It provides support for reading/writing data to/from an HBase
 cluster when bound to a Cascading data processing flow.

 Cascading is a feature rich API for defining and executing complex,
 scale-free, and fault tolerant data processing workflows on a Hadoop
 cluster. It can be found at the following location:

   http://www.cascading.org/

 HBase is the Hadoop database. Its an open-source, distributed,
 column-oriented store modeled after the Google paper on Bigtable.

   http://hadoop.apache.org/hbase/

Building

 This version could be built by using apache maven:
 mvn package
 
 Note that cloudera's maven repositories are used in order to resolve dependencies.

Using

  The cascading-hbase.jar file should be added to the "lib"
  directory of your Hadoop application jar file along with all
  Cascading dependencies.

  See the TestHBaseDynamic and TestHBaseStatic unit tests for sample code on using the HBase taps,
  schemes and helpers in your Cascading application.

License

  Copyright (c) 2009 Concurrent, Inc.

  This work has been released into the public domain
  by the copyright holder. This applies worldwide.

  In case this is not legally possible:
  The copyright holder grants any entity the right
  to use this work for any purpose, without any
  conditions, unless such conditions are required by law.