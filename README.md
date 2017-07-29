# geldit_time_series

To run:
hadoop jar geldit-0.0.1-SNAPSHOT.jar main.java.geldit.GelditAnalytic <input_path> <output_path> [parquet|text]

mapreduce1 directory contains implementation with mr1 api which is compatible with mr2, it uses deprecated parquet outputformat.
mapreduce2 directory contains implementation with mr2 api, works with mr2 only since it uses avroparquet outputformat that is not compatible with mr1
