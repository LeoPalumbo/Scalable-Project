# Scalable-Project

##BUILD
```bash
$ sbt assembly
```

##RUN
```bash
$ spark-submit --class main ./target/scala-VERSION/FILE.jar PARAMETER_LIST

#example of run
$ spark-submit --class main ./target/scala-2.12/HelloWorldSpark-assembly-1.0.jar
```