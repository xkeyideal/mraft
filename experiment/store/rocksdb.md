参考出处：https://www.jianshu.com/p/8e0018b6a8b6

## level compaction 参数

1. level0_file_num_compaction_trigger：当 level 0 的文件数据达到这个值的时候，就开始进行 level 0 到 level 1 的 compaction。所以通常 level 0 的大小就是 write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger。
   
2. max_bytes_for_level_base 和 max_bytes_for_level_multiplier：max_bytes_for_level_base 就是 level1 的总大小，在上面提到，我们通常建议 level 1 跟 level 0 的 size 相当。上层的 level 的 size 每层都会比当前层大 max_bytes_for_level_multiplier 倍，这个值默认是 10，通常也不建议修改。
   
3. target_file_size_base 和 target_file_size_multiplier：target_file_size_base 则是 level 1 SST 文件的 size。上面层的文件 size 都会比当前层大 target_file_size_multiplier 倍，默认 target_file_size_multiplier 是 1，也就是每层的 SST 文件都是一样的。增加 target_file_size_base 会减少整个 DB 的 size，这通常是一件好事情，也通常建议 target_file_size_base 等于 max_bytes_for_level_base / 10，也就是 level 1 会有 10 个 SST 文件。
   
4. compression_per_level：使用这个来设置不同 level 的压缩级别，通常 level 0 和 level 1 不压缩，更上层压缩。也可以对更上层的选择慢的压缩算法，这样压缩效率更高，而对下层的选择快的压缩算法。TiKV 默认全选择的 lz4 的压缩方式。
   
5. num_levels：预计的 level 的层数，如果实际的 DB level 超过了这么多层，也是安全的。默认是 7，实际也不会有超过这么多层的数据

## Flush 参数

对于新插入的数据，RocksDB 会首先将其放到 memtable 里面，所以 RocksDB 的写入速度是很快的。当一个 memtable full 之后，RocksDB 就会将这个 memtable 变成 immutable 的，然后用另一个新的 memtable 来处理后续的写入，immutable 的 memtable 就等待被 flush 到 level 0。也就是同时，RocksDB 会有一个活跃的 memtable 和 0 或者多个 immutable memtable

1. write_buffer_size：memtable 的最大 size，如果超过了这个值，RocksDB 就会将其变成 immutable memtable，并在使用另一个新的 memtable。
   
2. max_write_buffer_number：最大 memtable 的个数，如果 active memtable full 了，并且 active memtable 加上 immutable memtable 的个数已经到了这个阀值，RocksDB 就会停止后续的写入。通常这都是写入太快但是 flush 不及时造成的。

3. min_write_buffer_number_to_merge：在 flush 到 level 0 之前，最少需要被 merge 的 memtable 个数。如果这个值是 2，那么当至少有两个 immutable 的 memtable 的时候，RocksDB 会将这两个 immutable memtable 先 merge，在 flush 到 level 0。预先 merge 能减小需要写入的 key 的数据，譬如一个 key 在不同的 memtable 里面都有修改，那么我们可以 merge 成一次修改。但这个值太大了会影响读取性能，因为 Get 会遍历所有的 memtable 来看这个 key 是否存在