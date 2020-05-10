# Name
[name]: #name

Single Memory-Mapped File

# Author
[author]: #author

Dragan Milic

# Summary
[summary]: #summary

Using a memory mapped file to store the database data.
Instead of using file read and write operations, we should be using a memory-mapped file.

# Motivation
[motivation]: #motivation

Usual syscalls to read/write data (such as _open()_/_read()_/_write()_/...) are designed for sequential processing of files.
In the case of a database, this is sub-optimal, since we need a random access to the file data.

# Design
[design]: #design

Data of the whole database should be stored in one file.
When the database is opened, this file is memory-mapped (using the _mmap()_ syscall) to a memory region.
After the file is memory-mapped, data can be accessed via memory access (in Golang using a byte slice).
Writing data to the file is as simple as copying data to that memory region.

## Advantages

Memory mapped files can be much larger than the available RAM, but can be accessed as if they were just sequential portion of memory.
This is achieved by leverages OS's paging mechanism, loading pages on access and keeping them paged until there is a need to free memory for other purposes.

One of the advantages is that mmap-ed files won't be handled by Golang's garbage collector and the overhead of a context switch will be only incurred if the page is not yet in RAM.
In addition, the task of caching of pages kept in memory are completely outsourced to the OS running the application, enabling it to use all available memory of the system if needed.

## Appending data

When a file is memory-mapped, one of the parameters is the size of the memory region for the map.
This size can be larger than the size of the file.
If the memory region is larger than the current file length, when the file grows, additional data is automatically available in the memory-mapped region.

We can use that to append data to existing database - we can just extend the file using _ftruncate()_ syscall.
Of course, calling a syscall will incur penalty of a context switch, so we should be growing the file by some fixed amount (e.g. 16 mb) and then repeat that next time we need more space.

# Drawbacks
[drawbacks]: #drawbacks

Biggest drawback of memory-mapped files is the consistency of the written data.
When data is written in memory-mapped page, it is not written back to the disk until either the page is needed by the OS, _munmap()_ is called to remove the memory mapping or _msync()_ system call instructs the kernel to write all "dirty" pages (pages with data changed) to disk.

When the file size is larger than the memory mapped region, we cannot access the additional data.
This can be only fixed either by calling _munmap()_ on the memory region and then _mmap()_ with the larger region size or using _mremap()_ syscall, which is not available in MacOSX.
Result of either approach could end up with memory region being allocated to a different address, which is inconvenient.
For this reason, we should be memory-mapping a region that is larger than the anticipated database size.

Another drawback is that accessing data beyond the end of file might end up with SIGSEGV terminating the process, hence we should be very cautious not to make that mistake.

# Alternatives
[alternatives]: #alternatives

Instead of using _mmap()_, we could use the standard _open()_, _read()_, _write()_ and _fseek()_ operations.
This would incurs the cost of context switch whenever one of this operations are performed, crippling the performance of the database.

# Prior art
[prior-art]: #prior-art

Practically all other databases use this approach to access data from files.
