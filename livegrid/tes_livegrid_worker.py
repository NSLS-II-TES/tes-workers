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
    def __init__(self, array_counter_data_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.array_counter_name = array_counter_data_key

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
        """
        In [15]: list_scans[0].descriptors[0]["data_keys"]["xs_channel1_rois_roi01_value"]
        Out[15]:
        {'source': 'PV:XF:08BM-ES{Xsp:1}:C1_ROI1:Value_RBV',
         'dtype': 'number',
         'shape': [],
         'precision': 4,
         'units': '',
         'lower_ctrl_limit': 0.0,
         'upper_ctrl_limit': 0.0,
         'object_name': 'xs'}
        """
        if self.run_uid == doc["run_start"]:
            log.debug("descriptor:")
            log.debug(pprint.pformat(doc))
            if self.array_counter_name in doc["data_keys"]:
                log.info("found the Array_Counter")
                self.array_counter_descriptor_uid = doc["uid"]
            else:
                log.info("no ArrayCounter")

            if "xs_channel1_rois_roi1_value" in doc["data_keys"]:
                self.roi_pv_name = doc["data_keys"]["xs_channel1_rois_roi1_value"]["source"]
                self.roi_pv,  = self.epics_context.get_pvs(self.roi_pv_name)
                log.info(f"found the ROI PV name: {self.roi_pv_name}")
        else:
            pass

        super().descriptor(doc)

    def event(self, doc):
        super().event(doc)
        raise Exception("wasn't expecting this")

    def event_page(self, doc):
        """
        Respond to array counter monitor events.

        Read ROI PV.

        In [41]: db[-1].descriptors[0]["data_keys"]["xs_channel1_rois_roi01_value"]
        Out[41]:
        {'source': 'PV:XF:08BM-ES{Xsp:1}:C1_ROI1:Value_RBV',
         'dtype': 'number',
         'shape': [],
         'precision': 4,
         'units': '',
         'lower_ctrl_limit': 0.0,
         'upper_ctrl_limit': 0.0,
         'object_name': 'xs'}

        In [42]: db[-1].descriptors[0]["data_keys"]["xs_channel1_rois_roi01_value_sum"]
        Out[42]:
        {'source': 'PV:XF:08BM-ES{Xsp:1}:C1_ROI1:ValueSum_RBV',
         'dtype': 'number',
         'shape': [],
         'precision': 4,
         'units': '',
         'lower_ctrl_limit': 0.0,
         'upper_ctrl_limit': 0.0,
         'object_name': 'xs'}

        In [16]: list_scans = list(db(plan_name="list_scan")
        In [17]: len(list_scans)
        Out[17]: 230
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


def livegrid_dispatcher(manager, busy_function, topics, bootstrap_servers, group_id, **kwargs):
    dispatcher = RemoteDispatcher(
        topics=topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id
    )

    dispatcher.subscribe(func=manager)

    dispatcher.start(work_during_wait=busy_function)


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
    argparser.add_argument(
        "--array-counter-data-key",
        type=str,
        help="Xspress3 array counter data key",
        default="ArrayCounter"
    )

    args_ = argparser.parse_args()
    print(args_)

    # factory('start', start_doc) -> List[Callbacks], List[SubFactories]
    def livegrid_document_router_factory(start_doc_name, start_doc):
        # create a DocumentRouter only for list_scans
        if start_doc["plan_name"] == "list_scan":
            log.info("we have a list_scan")
            livegrid_document_router = LiveGridDocumentRouter(
                array_counter_data_key=args_.array_counter_data_key,
                #array_counter_name="ArrayCounter"
            )
            livegrid_document_router(start_doc_name, start_doc)
            return [livegrid_document_router], []
        else:
            log.info("not a list_scan!")
            return [], []

    livegrid_dispatcher(
        manager=RunRouter(factories=[livegrid_document_router_factory]),
        busy_function=lambda: plt.pause(1),
        **vars(args_)
    )
