import tempfile

from geoseeq.result.file_chunker import FileChunker


def test_file_chunker_reads_chunks_correctly():
    data = b"abcdefghij"  # 10 bytes
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    chunker = FileChunker(tmp_path, chunk_size=3)
    assert chunker.n_parts == 4
    # Without preloading
    assert chunker.get_chunk(0) == b"abc"
    assert chunker.get_chunk_size(3) == 1
    chunker.load_all_chunks()
    assert len(chunker.loaded_parts) == 4
    assert chunker.get_chunk(2) == b"ghi"
