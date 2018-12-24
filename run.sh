#!/usr/bin/env bash
#
# Submit a spark application
#
usage() {
  cat<<EOF
Usage: $(basename "$0") <jar> <class> [args...]

ex:
  spark-submit \\
     --master local \\
     --name MyApp \\
     --class com.example.spark.TemperatureMinMax \\
     \$(pwd)/target/scala-2.11/myapp-assembly-0.0.1.jar \\
     \$(pwd)/data/okayama-2010-2018-temperature.csv \$(pwd)/out 201
EOF
}

JAR="$1"   ; shift
CLASS="$1" ; shift

spark-submit --master "${SPARK_MASTER_HOST:-local}" \
             --name MyApp \
             --class "$CLASS" \
             "$JAR" "$@"
