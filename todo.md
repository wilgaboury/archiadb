bio: default to fdatasync when alloc has not been called.

for improved remaining locked mem use procedure:
1. Read the soft limit with getrlimit(RLIMIT_MEMLOCK, ...) or prlimit().
2. Read your current locked amount from /proc/self/status, field VmLck.
3. Subtract, ideally on page boundaries.

Bug: The way that root but of being committed by lalloc will prematurely commit transaction content which is wrong, need to separate lalloc header changes from buffered content changes.

Outstanding Pre 1.0 Work
- Support other platforms - Windows, OSX, BSD, Posix
- Add synchronous unsafe C API (pollster)