import sys

from etl.pipeline import ETLPipeline
from etl.utils.logger import setup_logging

setup_logging()


def main() -> int:
    with ETLPipeline() as pipeline:
        pipeline.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
