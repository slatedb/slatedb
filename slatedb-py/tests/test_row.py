from slatedb import SlateDB


def test_get_row(db: SlateDB):
    key = b"key1"
    value = b"value1"
    db.put(key, value)

    row = db.get_row(key)
    assert row is not None
    assert row.key == key
    assert row.value == value
    assert row.seq > 0
    assert row.create_ts is not None

    non_existent = db.get_row(b"non_existent")
    assert non_existent is None

def test_next_row(db: SlateDB):
    key1 = b"key1"
    value1 = b"value1"
    key2 = b"key2"
    value2 = b"value2"

    db.put(key1, value1)
    db.put(key2, value2)

    iterator = db.scan(b"key")
    
    row1 = iterator.next_row()
    assert row1 is not None
    assert row1.key == key1
    assert row1.value == value1
    assert row1.seq > 0

    row2 = iterator.next_row()
    assert row2 is not None
    assert row2.key == key2
    assert row2.value == value2
    assert row2.seq > 0

    row3 = iterator.next_row()
    assert row3 is None
