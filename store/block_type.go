package store

type BlockType byte

const BTreeMetaBlockType BlockType = 1
const BTreeNodeBlockType BlockType = 2
const BTreeLeafBlockType BlockType = 3
