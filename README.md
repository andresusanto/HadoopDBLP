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
hadoop jar <jar file> AsusEngine <input file in hdfs> <output file in hdfs> <xml Document Tag>

# single search example
hadoop jar bin/dblpc.jar AsusEngine /dblp/dblp.xml /user/triplex/dblp phdthesis

# multiple search example (article , books, and inproceedings at the same time) is separated by using ,
hadoop jar bin/dblpc.jar AsusEngine /dblp/dblp.xml /user/triplex/dblp article,inproceedings,book
```
## Execution Result
![Execution Result](/../screenshoot/screenshoot/ss.PNG?raw=true "Result")

**Executed Command:**

```
hadoop jar bin/dblpc.jar AsusEngine /dblp/dblp.xml /user/triplex/dblp article,inproceedings
```

**Output:**

```
_inproceedings_	1714171
_article_	1375638
H. Vincent Poor	1165
Wei Wang	1124
Yan Zhang	1050
Wei Liu	1033
Wen Gao	932
Philip S. Yu	893
Thomas S. Huang	841
Wei Zhang	831
Lei Wang	829
Yang Yang	826
Lajos Hanzo	820
Jing Li	807
Chin-Chen Chang	805
```

**Note:** `_{DOCUMENT}_` is the number of document type in the file (in this example is the numbers of inproceedings and article)
