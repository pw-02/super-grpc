from queue import Queue

class CustomQueue(Queue):
    def remove_item(self, value):
        with self.mutex:
            try:
                self.queue.remove(value)
                return True  # Item removed successfully
            except ValueError:
                return False  # Item not found in the queue

# Example usage:
q = CustomQueue()
q.put(1)
q.put(2)
q.put(3)

value_to_remove = 2
if q.remove_item(value_to_remove):
    print(f"Item {value_to_remove} removed from the queue.")
else:
    print(f"Item {value_to_remove} not found in the queue.")
