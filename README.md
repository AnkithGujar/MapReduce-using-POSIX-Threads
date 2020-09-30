# MapReduce using POSIX Threads

## Description:

This is an implementation of MapReduce as a producer-consumer problem with mutex synchronization using POSIX Threads. It is implemented in a way that there is one producer thread (Mapper) and the number of slots as well as the number of consumer threads (Reducers) can be set by the user in runtime.