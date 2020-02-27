import argparse
import logging
import pprint

import matplotlib
matplotlib.use('qt5agg')
import matplotlib.backends.backend_qt5
import matplotlib.pyplot as plt

import numpy as np

from bluesky_kafka import RemoteDispatcher
from caproto.threading.client import Context
from event_model import DocumentRouter, RunRouter


matplotlib.backends.backend_qt5._create_qApp()

log = logging.getLogger("tes.worker.livegrid")
log.addHandler(logging.StreamHandler())
log.setLevel("DEBUG")

"""
Typical usages:

python tes_livegrid_server.py \
    --topics srx.bluesky.documents \
    --bootstrap-servers 10.0.137.8:9092 \
    --group-id srx.livegrid

python tes_livegrid_server.py \
    --topics tes.bluesky.documents \
    --bootstrap-servers 10.0.137.8:9092 \
    --group-id tes.livegrid
"""


class LiveGridDocumentRouter(DocumentRouter):
    def __init__(self, array_counter_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.array_counter_name = array_counter_name

        self.fig = None
        self.ax = None
        self.axes_image = None
        self.image_array = None

        self.run_uid = None
        self.array_counter_descriptor_uid = None
        self.roi_pv_name = None

        self.epics_context = Context()
        self.roi_pv = None

    def start(self, doc):
        log.debug("start")
        log.debug(pprint.pformat(doc))
        log.info(f"starting a LiveGrid for run {doc['uid']}")
        self.run_uid = doc["uid"]

        # TODO: is this information in the document?
        self.image_array = np.zeros((10, 10))
        self.fig, self.ax = plt.subplots(nrows=1, ncols=1)
        self.axes_image = self.ax.imshow(self.image_array)
        plt.show(block=False)

        super().start(doc)

    def descriptor(self, doc):
        if self.run_uid == doc["run_start"]:
            log.debug("descriptor:")
            log.debug(pprint.pformat(doc))
            if self.array_counter_name in doc["data_keys"]:
                log.info("found the Array_Counter")
                self.array_counter_descriptor_uid = doc["uid"]
            else:
                log.info("no ArrayCounter")
        else:
            pass

        super().descriptor(doc)

    def event(self, doc):
        super().event(doc)
        raise Exception("wasn't expecting this")

    def event_page(self, doc):
        """
        Respond to array counter monitor events.
        """
        log.debug("event page:")
        log.debug(pprint.pformat(doc))
        if doc["descriptor"] == self.array_counter_descriptor_uid:
            log.debug("plot a point!")
            array_counter = doc["data"]["ArrayCounter"][0]
            row = (array_counter - 1) // 10
            col = (array_counter - 1) % 10
            log.debug("array_counter: %s, row: %s, column: %s", array_counter, row, col)
            roi_value = self.roi_pv.read()
            self.image_array[row, col] = roi_value.data[0]
            log.debug(self.image_array)
            self.ax.imshow(self.image_array)
            self.fig.canvas.draw_idle()
            #self.axes_image.set_data(self.image_array)
        else:
            log.debug("not an array counter event")

        super().event_page(doc)

    def stop(self, doc):
        log.debug("stop:")
        log.debug(pprint.pformat(doc))

        super().stop(doc)


def livegrid_dispatcher(manager, busy_function, topics, bootstrap_servers, group_id):
    dispatcher = RemoteDispatcher(
        topics=topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id
    )

    dispatcher.subscribe(func=manager)

    dispatcher.start(busy_function=busy_function)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "--topics",
        type=str,
        help="comma-delimited list",
        nargs="+",
        default=["tes.bluesky.documents"],
    )
    # CMB01 broker: 10.0.137.8:9092
    argparser.add_argument(
        "--bootstrap-servers",
        type=str,
        help="comma-delimited list",
        default="10.0.137.8:9092",
    )
    argparser.add_argument(
        "--group-id", type=str, help="a string", default="tes-livegrid-worker"
    )

    args_ = argparser.parse_args()
    print(args_)

    # factory('start', start_doc) -> List[Callbacks], List[SubFactories]
    def livegrid_document_router_factory(start_doc_name, start_doc):
        # create a DocumentRouter only for fly scans (?)
        # TODO: does the flyscan start document have an identifying key?
        if "livegrid" in start_doc:
            log.info("we have a livegrid scan")
            livegrid_document_router = LiveGridDocumentRouter(
                array_counter_name="ArrayCounter"
            )
            livegrid_document_router(start_doc_name, start_doc)
            return [livegrid_document_router], []
        else:
            log.info("not a fly scan!")
            return [], []

    livegrid_dispatcher(
        manager=RunRouter(factories=[livegrid_document_router_factory]),
        busy_function=lambda: plt.pause(1),
        **vars(args_)
    )
