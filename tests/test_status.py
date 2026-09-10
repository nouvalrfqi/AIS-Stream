from stream_processor.status import derive_status


def test_anchored_priority():
    assert derive_status(1, 0.0) == "ANCHORED"
    assert derive_status(1, 8.0) == "ANCHORED"


def test_moored_priority():
    assert derive_status(5, 0.0) == "MOORED"
    assert derive_status(5, 8.0) == "MOORED"


def test_stopped_when_nav_zero_and_sog_zero():
    assert derive_status(0, 0) == "STOPPED"


def test_underway_when_nav_zero_sog_positive():
    assert derive_status(0, 5.5) == "UNDERWAY"


def test_underway_sailing():
    assert derive_status(8, 3.0) == "UNDERWAY"


def test_unknown_when_other_nav_status():
    assert derive_status(15, 0.0) == "UNKNOWN"
    assert derive_status(9, 0.0) == "UNKNOWN"


def test_unknown_when_none():
    assert derive_status(None, None) == "UNKNOWN"