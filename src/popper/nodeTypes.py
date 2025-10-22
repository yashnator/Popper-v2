from .graph import Node


class RootNode(Node):
    def __init__(self, id, num_threads=1):
        super().__init__(id, None, num_threads)
        self._stop_flag = True

    def _run(self):
        return
