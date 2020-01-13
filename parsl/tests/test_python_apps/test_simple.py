import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_ipp import config

from concurrent.futures import Future

from typing import Dict, Union

@python_app
def increment(x):
    return x + 1


@python_app
def slow_increment(x, dur):
    import time
    time.sleep(dur)
    return x + 1


def test_increment(depth=5):
    futs = {0: 0} # type: Dict[int, Union[int, Future]]
    for i in range(1, depth):
        futs[i] = increment(futs[i - 1])

    # this is a slightly awkward rearrangement: we need to bind f so that mypy
    # can take the type property proved by isinstance and carry it over to
    # reason about if f.result() valid.
    x = sum([f.result() for i in futs for f in [futs[i]] if isinstance(f, Future)])
    assert x == sum(range(1, depth)), "[TEST] increment [FAILED]"


def test_slow_increment(depth=5):
    futs = {0: 0} # type: Dict[int, Union[int, Future]]
    for i in range(1, depth):
        futs[i] = slow_increment(futs[i - 1], 0.01)

    x = sum([f.result() for i in futs for f in [futs[i]] if isinstance(f, Future)])

    assert x == sum(range(1, depth)), "[TEST] slow_increment [FAILED]"


if __name__ == '__main__':
    parsl.clear()
    parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", default="5",
                        help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    tests = [test_increment, test_slow_increment]
    for width in [10, 100, 1000]:
        for test in tests:
            try:
                test(depth=int(width))

            except AssertionError as e:
                print("[TEST]  %s width:%s [FAILED]" % (test.__name__, width))
                print(e)

            else:
                print("[TEST]  %s width:%s type [SUCCESS]" %
                      (test.__name__, width))
