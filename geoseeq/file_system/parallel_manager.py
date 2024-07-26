
from concurrent.futures import ThreadPoolExecutor, as_completed


class AbnormalObjectListManager:

    def __init__(self, threads=1):
        self.n_threads = threads
        self.queue = []

    def list_abnormal_objects(self):
        return self._list_abnormal_objects_multi_threaded()
    
    def add_object(self, obj):
        self.queue.append(obj)
        return self
    
    def traverse_object_tree(self):
        """Return a list of all onjects in the queue and their descendants."""
        my_queue = [x for x in self.queue]
        i = 0
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            while i < len(my_queue):
                futures = []
                for obj in my_queue[i:]:
                    futures.append(executor.submit(obj.yield_child_objects))
                    i += 1
                for future in as_completed(futures):
                    my_queue += future.result()
        return my_queue
    
    def _list_abnormal_objects_multi_threaded(self):
        my_queue = set(self.traverse_object_tree())
        out, futures = [], []
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            for obj in my_queue:
                futures.append(executor.submit(obj.atomic_abnormal_status))
            for future in as_completed(futures):
                out += future.result()
        return out
    
