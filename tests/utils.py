
def assert_frame_equal(left, right):
    try:
        assert left.subtract(right).count() == 0
    except AssertionError as ae:
        print('Left:')
        left.show()
        print('Right:')
        right.show()
        raise ae
