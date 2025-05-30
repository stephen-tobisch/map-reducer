# Map Reduce

This program takes a .txt file and returns the number of occurrences of each word in the file. To accomplish this, it uses the map-reduce model. This programming model is often used for processing large amounts of data because it allows parallelization of the computation across multiple machines using multiple processes (though in this case just a single machine is being used with multiple processes).

The program is divided into 4 separate phases: master, mapper, shuffle, and reducer. The master is the main program that controls the phases. It creates mappers and reducers to parse the data. Each mapper and reducer is its own process. The mapper takes each word and creates a list which counts all the occurrences of the word. Then a shuffle phase happens which partitions the files with a hash function and sends them to the reducers through a message queue. The reducer sums the list of occurrences into a single value and writes it into a single file (per reducer).

Several helper functions are used to parse through files and send and receive elements of the specific size (1024 bytes) to the message queue. `sendChunkData()` sends the data to a message queuein a round robin fashion. `getChunkData()` then receives this datafrom the message queue. `getInterData()` receives the path to the text files from each reducer.

### Compilation

The program can be compiled with the makefile in the root directory. The compiler being used is gcc, which must be installed on the system first.

```
make
```

### Execution

The main executable created is mapreduce. This can be run with the following command

```
./mapreduce <# of mappers> <# of reducers> <input file>
```

There is a sample .txt file located in the test directory. To run the program with this test file with 5 mappers and 2 reducers, for example, the command would be

```
./mapreduce 5 2 ./test/T1/F1.txt
```

This test command can also be run with the makefile.

```
make t1
```

The results can be seen in the output directory.
