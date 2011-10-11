package cascading.hbase;

import cascading.scheme.Scheme;

public abstract class HBaseGenScheme extends Scheme
{
	public abstract String[] getFamilyNames();
}
