from tqdm import tqdm
from os.path import basename

class TQBar:

    def __init__(self, pos, desc) -> None:
        self.n_bars = 0
        self.pos = pos
        self.desc = desc
        self.bar = None

    def set_num_chunks(self, n_chunks):
        self.n_bars = n_chunks
        self.bar = tqdm(total=n_chunks,
                        position=self.pos, desc=self.desc, leave=False, unit="B",
                        unit_scale=True, unit_divisor=1024)

    def update(self, chunk_num):
        self.bar.update(chunk_num)


class PBarManager:

    def __init__(self):
        self.n_bars = 0
        self.pbars = []

    def get_new_bar(self, filepath):
        self.n_bars += 1
        return TQBar(self.n_bars, basename(filepath))