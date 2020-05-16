# Name
[name]: #name

Database Blocks

# Author
[author]: #author

Dragan Milic

# Summary
[summary]: #summary

This is a description of how single memory mapped file is split into smaller units (blocks) that can be used to store data.

# Motivation
[motivation]: #motivation

Plan for building the structure of l5db (spoiler alert!) is to use nested B-Trees with data at the lowest level leaves.
Hence we are introducing notion of blocks as an atomic piece of information in l5db.

Blocks solve following requirements.

# Manageable chunks of data

Just having one big piece of memory is not very useful for storing a B-Tree, since B-Tree operates on small chunks of memory for nodes, also data should be split into chunks with an upper bound in size, which will enable us to have streaming-like interface for processing large amounts of data (e.g. 100mb data as a leaf).

# Memory management

Memory mapped file enables us keep more data in our database than can be fitted into available RAM.
Still, the available space will be exhausted really quickly if we don't have a strategy for allocating and re-using de-allocated parts of the file
At the same time we should avoiding as much as possible memory fragmentation.
Introducing blocks help with with task by fitting nicely into a buddy memory allocator method [1], that is also used by the Linux kernel.

# Design
[design]: #design

Here we describe how we are splitting the big memory-mapped file into smaller pieces of memory that can be used for both storing of the meta data (B-Tree nodes) and the actual data (values).

## Structure of a block

Every block has it's address, which is the offset of the first byte of the block relative to the beginning of the memory-mapped file.

Every block consists of a header (2 bytes) and payload.

Header describes the type of the data contained in the block (block type) and the size of the block (as exponent for the base of 2).

Size of the block is the total length of the block (including the header).

Layout of a block can be represented as 

```

+--------+--------+--------///----------+
| Length | Type   | Data ...            |
+--------+--------+--------///----------+
  1 byte   1 byte   2**Length - 2 bytes
```

## Block length

We have chosen to store length of the block as a power of 2.
This has the advantage of a very compact representation of the block length (only one byte)
and will enable us to implement buddy algorithm for allocation (subject of one of the following RFCs).

The downside of using a power of 2 for the block length is that space will be wasted when the data is just a byte or two larger than available in smaller block.
Worst case scenario would be up to 50% of unused data in a block.
On average this waste should be around 25% given a even distribution of data length.

Knowing the 'boundary' of a block will be very useful when we design an API to access blocks, since we can easily enforce boundary checking, guaranteeing that SIGSEGV by accessing beyond the end of the memory mapped file won't be happening due to a coding error..

## Block type

Every block has one byte in header describing the type of the block.
This is chosen since we will be storing the database structure in nested B-Tree(s), requiring us to 'know' the type of a value (if it is data or a child B-Tree for example).

We will be introducing block types in the following RFCs whenever we have the need to introduce a new type of data.

# Drawbacks
[drawbacks]: #drawbacks

We are 'wasting' at least 2 bytes per each block and in worst case 50% of the data in the memory-mapped file by choosing to store the block length as a power of 2.

# Alternatives
[alternatives]: #alternatives

We could be using only addresses to data structures, without defining block header in the first place.
Drawback of doing so is that SIGSEGV can happen if we're not careful.
Also implementing an allocator almost always implies having a similar header before the allocated memory.


# Prior art
[prior-art]: #prior-art

Most of databases have fixed sized data blocks. 
Usually size of 4kb is chosen for a block, since that coincides with the memory page size.
In our case we will be needing smaller blocks, so that we can support storing very small chunks of data (e.g. 32 bytes ethereum transaction hash).


# References

[1] Kenneth C. Knowlton. A fast storage allocator. Communications of the ACM, 8(10):623–624, 1965.
