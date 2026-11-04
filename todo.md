bio:
default to fdatasync when alloc has not been called.

for improved remaining locked mem use procedure:
1. Read the soft limit with getrlimit(RLIMIT_MEMLOCK, ...) or prlimit().
2. Read your current locked amount from /proc/self/status, field VmLck.
3. Subtract, ideally on page boundaries.