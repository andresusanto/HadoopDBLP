# Hadoop DBLP
DBLP Processing using Hadoop's MapReduce algorithm

## Compiling
To compile this project make sure that hadoop can access `javac` compiler. To do that, issue the following command:
```bash
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```

Then you can compile HadoopDBLP by issuing the following command:

```bash
mkdir bin
hadoop com.sun.tools.javac.Main -d bin AsusEngine.java XmlInputFormat.java
cd bin
jar cf dblpc.jar AsusEngine*.class XmlInputFormat*.class
cd ..
```

After issuing previous command, you will find a jar file (dblpc.jar) in `bin` folder.

## Running
To run this project, issue the following command:
```bash
hadoop jar <jar file> AsusEngine <input file in hdfs> <output file in hdfs>

# example
hadoop jar bin/dblpc.jar AsusEngine /dblp/dblp.xml /user/triplex/dblp
```
