\dnS+ list schemas

```postgres-psql
SET client_min_messages TO DEBUG;
set pljava.libjvm_location to '/usr/pgsql-10/ext/jdk1.8.0_91/jre/lib/amd64/server/libjvm.so';
set pljava.vmoptions to '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5580';
SET pljava.classpath TO '/usr/pgsql-10/share/pljava/pljava-1.6.0-SNAPSHOT.jar';
load '/usr/pgsql-10/lib/libpljava-so-1.6.0-SNAPSHOT.so';


select sqlj.remove_jar('dp', true);

select sqlj.install_jar('file:///usr/pgsql-10/share/pljava/postgresql-udfs-0.0.2.jar', 'dp', true);
select sqlj.set_classpath('dp','dp');


select sqlj.install_jar('file:///usr/pgsql-10/share/pljava/pljava-examples-1.6.0-SNAPSHOT.jar', 'javatest', true);
select sqlj.set_classpath('javatest','javatest');
```

<pre>   
DEBUG:  Added JVM option string "-Dvisualvm.display.name=PL/Java:24276:dm"
DEBUG:  Added JVM option string "vfprintf"
DEBUG:  Added JVM option string "-Xrs"
DEBUG:  Added JVM option string "-Djava.class.path=/usr/pgsql-10/share/pljava/pljava-1.6.0-SNAPSHOT.jar"
DEBUG:  creating Java virtual machine
DEBUG:  successfully created Java virtual machine
DEBUG:  checking for a PL/Java Backend class on the given classpath
DEBUG:  successfully loaded Backend class
NOTICE:  PL/Java loaded
DETAIL:  versions:
PL/Java native code (1.6.0-SNAPSHOT)
PL/Java common code (1.6.0-SNAPSHOT)
Built for (PostgreSQL 10.6 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-28), 64-bit)
Loaded in (PostgreSQL 10.7 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-36), 64-bit)
Java(TM) SE Runtime Environment (1.8.0_91-b14)
Java HotSpot(TM) 64-Bit Server VM (25.91-b14, mixed mode)
DEBUG:  CREATE TABLE will create implicit sequence "jar_repository_jarid_seq" for serial column "jar_repository.jarid"
DEBUG:  building index "pg_toast_33145_index" on table "pg_toast_33145"
DEBUG:  CREATE TABLE / PRIMARY KEY will create implicit index "jar_repository_pkey" for table "jar_repository"
DEBUG:  building index "jar_repository_pkey" on table "jar_repository"
DEBUG:  CREATE TABLE / UNIQUE will create implicit index "jar_repository_jarname_key" for table "jar_repository"
DEBUG:  building index "jar_repository_jarname_key" on table "jar_repository"
DEBUG:  CREATE TABLE will create implicit sequence "jar_entry_entryid_seq" for serial column "jar_entry.entryid"
DEBUG:  building index "pg_toast_33158_index" on table "pg_toast_33158"
DEBUG:  CREATE TABLE / PRIMARY KEY will create implicit index "jar_entry_pkey" for table "jar_entry"
DEBUG:  building index "jar_entry_pkey" on table "jar_entry"
DEBUG:  CREATE TABLE / UNIQUE will create implicit index "jar_entry_jarid_entryname_key" for table "jar_entry"
DEBUG:  building index "jar_entry_jarid_entryname_key" on table "jar_entry"
DEBUG:  CREATE TABLE / PRIMARY KEY will create implicit index "jar_descriptor_pkey" for table "jar_descriptor"
DEBUG:  building index "jar_descriptor_pkey" on table "jar_descriptor"
DEBUG:  CREATE TABLE / PRIMARY KEY will create implicit index "classpath_entry_pkey" for table "classpath_entry"
DEBUG:  building index "classpath_entry_pkey" on table "classpath_entry"
DEBUG:  CREATE TABLE will create implicit sequence "typemap_entry_mapid_seq" for serial column "typemap_entry.mapid"
DEBUG:  CREATE TABLE / PRIMARY KEY will create implicit index "typemap_entry_pkey" for table "typemap_entry"
DEBUG:  building index "typemap_entry_pkey" on table "typemap_entry"
LOAD

</pre>

