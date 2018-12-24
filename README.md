Basic Spark Sample Project
==========================

## Make a package

```bash
sbt assembly
```

## Generate a eclipse project

```bash
sbt eclipse
```

## Submit a spark application

```bash
spark-submit \
     --master local \
     --name MyApp \
     --class com.example.spark.TemperatureMinMax \
     \$(pwd)/target/scala-2.11/myapp-assembly-0.0.1.jar \
     \$(pwd)/data/okayama-2010-2018-temperature.csv \$(pwd)/out 201
```

## See output

```
cat ./out/part-*
```
