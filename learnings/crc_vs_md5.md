# Why CRC is used in wal instead of MD5 or other hashing?
[source](https://www.reddit.com/r/DataHoarder/comments/19c1i83/comment/kivnuc3/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button)

For file hashing purposes, two things generally matter:

What are the odds of two random files having the same hash? (entropy/collision)

How hard is it to intentionally create two different files with the same hash? (collision in the context of security)

2 has security implications if users are validating the integrity of a file based on its hash, and a malicious file matches a non-malicious hash. Imagine something like an anti-P2P company wrecking Bittorrent by being able to send random fake data that peoples' torrent client thinks is valid because its hash matches and continues to propogate that bogus data to other users. You could poison a torrent and kill it or spread malware. The SHA hashing algorithm was made with the intent to make it difficult to create intentional collisions, and is historically used for cryptographic functions. For things like storing password hashes, intentionally slow and expensive hash algos can be used to make it hard to brute force.

But if you're only worried about checking if your files are the same now as they were before, you just use a hash with enough bits of entropy that the chance of random collision is super low.

CRC32 is a simple old/classic hash. It is trivial to force two files to have the same CRC32. It has 32 bits of entropy, which means there are 2^32 or ~4.3 billion unique possible values. If you have tens of millions of files then the chance of a one having the same hash as another is non-trivial (see: birthday problem). However, if a file is randomly corrupted then the odds of you not being able to detect it is only 1 in 4.3 billion. For the purpose of detecting corruption and without worrying about intentional sabatoge, it's perfectly fine. But there's no compelling reason to use it when you have better algorithms at your disposal.

# Hash vs cryptographic hash function.
[source](https://security.stackexchange.com/questions/11839/what-is-the-difference-between-a-hash-function-and-a-cryptographic-hash-function)

1. every cryptographic hash function is a hash function but every hash function is not cryptographic hash function 
2. hash functions take a arbitary size input and generate fixed size random string as output. For same input the output remains same. hash functions try to avoid collisions for non-malicious inputs. They are normally used to find errors in transmissions (CRC - Cyclinc Redundancy Checks), Organizing objects into buckets in hash table etc .
3. Cryptographic hash functions are hash functions which provide additional properties. The additional properties include ([source](https://en.wikipedia.org/wiki/Cryptographic_hash_function))
a. Pre-image resistance: given a hash h, it should be difficult to find the message m, such that hash(m) = h.
b. Second Pre-image resistance: given a message m1 it should be hard to find another message m2 such that hash(m1)=hash(m2)
c. Collision Resistance: It should be difficult to find two messages m1 and m2 such that hash(m1)=hash(m2)