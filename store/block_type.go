package store

type BlockType byte

const BTreeMetaBlockType BlockType = 1
const BTreeInternalNodeBlockType BlockType = 2
const BTreeLeafBlockType BlockType = 3
const SequentialMetaBlockType BlockType = 4
const SequentialDataBlockType BlockType = 5
