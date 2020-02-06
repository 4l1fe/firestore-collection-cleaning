import logging
from threading import Thread, Event
from pathlib import Path
from queue import Queue, Empty

from google.cloud import firestore
from retrying import retry


BASE_DIR = Path(__file__).parent
KEY_FILE = BASE_DIR / 'key.json'
THREADS_COUNT = 1
BATCH_SIZE = 250
EMPTY_QUEUE_TIMEOUT = 2


def retry_on_exception(exception):
    logging.error(f'retry {exception}')
    return isinstance(exception, Exception)


def retry_on_result(result):
    logging.info('no retry on finish')
    return False


def delete(queue, finished):
    logging.info('start deleting')
    db = firestore.Client.from_service_account_json(KEY_FILE.as_posix())
    batch = db.batch()

    while True:
        try:
            refs = queue.get(timeout=EMPTY_QUEUE_TIMEOUT)
        except Empty:
            logging.info('empty')
            if finished.is_set():
                break
            continue

        for ref in refs:
            batch.delete(ref)
        logging.info(f'delete batch, refs count {len(refs)}')
        batch.commit()

    logging.info('stop deleting')


def main(collection, threads_count):
    logging.info('start cleaning')

    docs_finished = Event()
    queue = Queue()
    db = firestore.Client.from_service_account_json(KEY_FILE.as_posix())

    deleters = []
    for i in range(1, threads_count + 1):
        t = Thread(name=f'DEL{i}', target=delete, args=(queue, docs_finished))
        t.start()
        deleters.append(t)

    @retry(retry_on_result=retry_on_result, retry_on_exception=retry_on_exception)
    def _make_batches():
        docs = db.collection(collection).stream()
        refs = []
        for i, doc in enumerate(docs, start=1):
            if i % BATCH_SIZE == 0:
                queue.put_nowait(refs.copy())
                logging.info(f'add a batch, queue size {queue.qsize()}')
                refs.clear()
            refs.append(doc.reference)

        if refs:
            queue.put(refs)
            logging.info('the last batch')

    _make_batches()
    docs_finished.set()

    for t in deleters:
        t.join()

    logging.info('cleaning finished')


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('collection')
    parser.add_argument('-tc', '--threads-count', default=THREADS_COUNT, type=int)
    parser.add_argument('-ll', '--log-level', default=logging.INFO, type=int)
    args = vars(parser.parse_args())
    logging.basicConfig(level=args.pop('log_level'), format='[%(asctime)s %(threadName)s %(levelname)s] %(message)s')
    main(**args)
