import logging
import sys

from etl.pipeline import ETLPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def main() -> int:
    with ETLPipeline() as pipeline:
        pipeline.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
