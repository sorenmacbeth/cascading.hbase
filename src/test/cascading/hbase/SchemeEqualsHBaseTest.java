/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.hbase;

import java.io.IOException;

import cascading.tuple.Fields;
import cascading.scheme.Scheme;

/**
 *
 */
public class SchemeEqualsHBaseTest extends HBaseTestCase
  {
  public SchemeEqualsHBaseTest()
    {
    super( 1, false );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    }

  public void testHBaseTapEquality() throws IOException
    {
    String[] familyNames = {"family1"};
    Fields keyFields = new Fields("keyfield1");
    Fields[] valueFields = new Fields[]{new Fields( "field1" ), new Fields( "field2" )};

    Scheme s1 = new HBaseScheme(keyFields, familyNames, valueFields);
    Scheme s2 = new HBaseScheme(keyFields, familyNames, valueFields);

    assertTrue("s1 and s2 don't refer to the same object", s1.equals(s2));
    }
  }