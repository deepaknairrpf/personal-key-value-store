# A File Based Key Value Store

## Description
A file based key value store written in Python, which exposes
`CRUD` functionalities as an API to be consumed as a context manager,
an example of which is presented in `driver.py`  

This supports dicts as values and also allows setting a TTL
on each key optionally. `CUD` operations implemented are thread-safe.

## Implementation
Each invocation of the `DataStore` creates two files.  
1. Value Store: A file where values are appended / written.
2. Meta Store: A hidden file which stores meta information such as  
the seek position of the value corresponding to each key and the TTL
information of the key.

APIs to the `DataStore` are implemented as a delegator pattern where
each API call gets delegated to the corresponding API call of a 
`FileMeta` object.

`FileMeta` objects maintains predominantly three datastructures:  

    1. The key-seek pos `dict` which maps the seek positions of all 
    the keys to the corresponding value objects. 
    This enables the `DataStore` to easily read the value object from
    the corresponding seek position.

    2. A free slot list which consists of discontinuos slots 
    present in the value store as a result of deletion.
    
    3. A min-heap which consists of expiry times of all keys whose TTL
    was specified during creation.

These data structures allow to carefully manage space and flush out
keys with expired TTLs.


## Assumptions
    1. Dict values passed by the clients are JSON serializable.
    
    2. Lack of encryption while writing to the files.
    
    3. TTL values are implemented as best-effort, i.e if the TTL
    of a key expires, it's not removed instantly and is still accessible
    until certain number of creates occur and new objects eventually
    replace the key-value pair.
    This looked like a necessary side effect.
    
    4. Deletion deletes the meta info from the `FileMeta` object
    and doesn't instantly clear the space from the value store.
    But since it's meta info is deleted, the key cannot be consumed even
    immediately but the value gets replaced in the value store only 
    when subsequent creates happen.
    Going the other way, would add on uneccessay complexity.
    
    5. Reads are optimized to be as fast as possible with minimal
    memory footprint. But, updates and deletes incur O(n) to rebuild the heap
    and create incurs O(log n) to set right the heap. Therefore, it's assumed 
    to be a read heavy usecase with rare updates and deletes.

## Validations
    1. Keys are less than or equal to 32 chars.

    2. Values are padded with '0' from left to pad it to a constant
    size of 16 KBs.

    3. Max size of the value store is 1 GB.

PS: All unit tests are available in the `tests` package.