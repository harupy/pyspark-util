
def assert_frame_equal(left, right):
    assert left.subtract(right).count() == 0
