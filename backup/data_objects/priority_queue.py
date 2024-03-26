import heapq
from queue import Empty as QueueEmpty  # Import QueueEmpty from the queue module

class PriorityQueue:
    def __init__(self):
        self._queue = []

    def enqueue(self, priority, item):
        heapq.heappush(self._queue, (priority, item))

    def dequeue(self):
        if not self.is_empty():
            priority, item = heapq.heappop(self._queue)
            return priority, item
        raise QueueEmpty

    def is_empty(self):
        return not bool(self._queue)