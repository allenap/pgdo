# To Do

Maybe consolidate all locking activity around the `coordinate::resource` system.
The backup and restore commands use this, but nothing else.

There are hard-to-separate responsibilities of locking and doing things. By that
I mean, for example:

- I want to make changes to a cluster, so I want to LOCK it exclusively. For
  this I might have to block, wait, retry several times, etc. – insert your
  favourite strategy here.
- Then, I want to start the cluster, which may also require retries, etc.
- Then, configure, stop, restart, do other things.
- Lastly:
  - Shutdown and UNLOCK, or
  - Leave running and UNLOCK, or
  - Leave running and LOCK SHARED, etc.

The code as I leave it now (2025-09-09) has some weird half-attempts at some of
these ideas, but it's not _whole_. I came back to this codebase having forgotten
some of the ideas I had previously and never finished working on, and starting
making changes without the full picture. This note is meant to help future me
realise what's going on.

Good luck, buddy!
