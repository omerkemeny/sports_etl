import logging
import time
import datetime
from typing import List, Tuple, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import JSONDecodeError
import requests

from config.consts import MAX_RETRIES, RETRY_DELAY, TIMEOUT

logger = logging.getLogger(__name__)


class FootballAPIExtractor:
    def __init__(self):
        self.timestamp = datetime.datetime.now().isoformat()

    def fetch_all_sources(self, source_configs: List[Dict[str, Any]], max_workers: int = 2) -> Dict[str, Any]:
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_source = {
                executor.submit(self._fetch_with_retry, config): config['name']
                for config in source_configs
            }
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    data, success = future.result()
                    if success:
                        results[source_name] = data
                        logger.info(f"Successfully fetched data from {source_name}")
                    else:
                        logger.error(f"Failed to fetch data for {source_name}")
                except Exception as e:
                    logger.error(f"Critical exception in thread for {source_name}: {e}")
        return results

    def _fetch_with_retry(self, config: Dict[str, Any]) -> Tuple[Optional[Any], bool]:
        for attempt in range(MAX_RETRIES):
            logger.debug(f"Fetching {config['name']} (attempt {attempt + 1})")
            result = self._try_fetch(config)
            if result is not None:
                return result, True
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))
        return None, False

    def _try_fetch(self, config: Dict[str, Any]) -> Optional[Any]:
        try:
            response = requests.get(
                config['url'],
                headers=config.get('headers'),
                params=config.get('params'),
                timeout=TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            name = config['name']
            if name.startswith('api-sports'):
                return data.get('response')
            if name.startswith('api-football'):
                if isinstance(data, dict) and 'error' in data:
                    logger.warning(f"{name} error: {data.get('message')}")
                    return None
            return data
        except JSONDecodeError as e:
            logger.warning(f"Malformed JSON from {config['name']}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.debug(f"Request error for {config['name']}: {e}")
            return None
