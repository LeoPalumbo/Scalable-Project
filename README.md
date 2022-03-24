# Scalable-Project

##BUILD
To make a build correctly we need to have Scala version >= 12.0.0 .

```bash
$ sbt assembly
```

##RUN
###PARAMETERS
* **PAR_MATRIX**: is a *boolean* (true or false)
* **PAR_JOINING**: is a *boolean* (true or false)
* **METRIC**: is a *char* (*p* or *s*)
* **MAX_SEQUENCES_PER_FILE**: is a *number* (must be *>=1* makes no sense otherwise)
```bash
$ spark-submit --class main ./target/scala-VERSION/FILE.jar PAR_MATRIX PAR_JOINING METRIC MAX_SEQUENCES_PER_FILE

#example of run
$ spark-submit --class main ./target/scala-2.12/HelloWorldSpark-assembly-1.0.jar true false p 10
```
