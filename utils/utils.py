def get_chunks(generator, size):
    iterator = iter(generator)
    yield from _head(iterator, size)


def _head(iterator, size):
    offset = 0
    i = 0
    data = []
    for chunk in iterator:
        data.append(chunk)
        i += 1
        if i >= size:
            yield data, offset
            offset += 1
            i = 0
            data = []
    if i:
        yield data, offset
