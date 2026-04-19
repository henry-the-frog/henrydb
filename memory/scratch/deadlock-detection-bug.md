# Lock Manager Deadlock Detection Bug

**Created:** 2026-04-19
**Uses:** 1

## Bug
`_wouldDeadlock()` BFS traversed `_txLocks` (resources the holder HOLDS) and searched those resources' queues for the holder. This is logically backwards.

## Why It Was Wrong
To detect deadlock, you need to find: what is each holder WAITING on? The waits-for graph goes: txId → holders_of_contested_resource → resources_they_are_waiting_on → holders_of_those_resources → ...

The old code found resources a holder already HOLDS, then checked if the holder appeared in those queues. This could only accidentally detect cycles in specific topologies (3-way worked by coincidence).

## Fix
BFS must scan ALL lock queues to find where each holder appears as a waiter, then follow the holders of those contested resources.

## Lesson
Waits-for graph direction matters: "X is waiting on Y" means X appears in some resource's queue, and Y holds that resource. The graph edge is from waiter to holder, not from holder to held-resource.

## Performance Note
The fixed BFS iterates `this._locks` (all resources) for each BFS node. This is O(resources × chain_length) per deadlock check. For large lock tables, a dedicated waits-for adjacency list would be O(1) per edge lookup. Fine for current scale.
