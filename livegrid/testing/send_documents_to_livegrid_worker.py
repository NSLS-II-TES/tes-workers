import argparse
import datetime
import logging
import time

from bluesky_kafka import Publisher
from event_model import compose_run


logging.basicConfig(level=logging.DEBUG)


"""
Local development:
Start docker if necessary.
    sudo systemctl start docker
Start a Kafka broker.
    docker-compose -f testing/bitnami-kafka-docker-compose.yml up
Add the necessary topic if automatic topic creation is not enabled:
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic srx.bluesky.documents
or check that the necessary topic exists:
    ./bin/kafka-topics.sh --list --zookeeper localhost:2181
Start simulated Xspress3.
    python testing/sim_xspress3.py --list-pvs
Start tes_livegrid_worker.py.
    python tes_livegrid_worker.py
Start send_documents_to_livegrid_worker.py
    python testing/send_documents_to_livegrid_worker.py
"""


def send_documents(topic, bootstrap_servers):
    print("send documents to kafka broker")
    kafka_publisher = Publisher(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        key="testing",
        producer_config={"enable.idempotence": False}
    )

    run_start_doc, compose_desc, compose_resource, compose_stop = compose_run()
    run_start_doc["scan_id"] = 1
    run_start_doc["plan_name"] = "list_scan"
    kafka_publisher("start", run_start_doc)

    # copied from run 588bed89-b8e9-4882-86b2-c9471612914e at SRX
    roi_event_descriptor_doc, compose_roi_event, compose_roi_event_page = compose_desc(
        data_keys={
            "xs_channel1_rois_roi1_value": {
                "source": "PV:XF:08BM-ES{Xsp:1}:C1_ROI1:Value_RBV",
                "dtype": "number",
                "shape": [],
                "precision": 4,
                "units": "",
                "lower_ctrl_limit": 0.0,
                "upper_ctrl_limit": 0.0,
            }
        },
        configuration={
            "xs_channel1_rois_roi1_value": {
                "data": {"xs_channel1_rois_roi1_value": 6201.48337647908},
                "timestamps": {"xs_channel1_rois_roi1_value": 1572730676.801648},
                "data_keys": {
                    "xs_channel1_rois_roi1_value": {
                        "source": "PV:XF:08BM-ES{Xsp:1}:C1_ROI1:Value_RBV",
                        "dtype": "number",
                        "shape": [],
                        "precision": 4,
                        "units": "",
                        "lower_ctrl_limit": 0.0,
                        "upper_ctrl_limit": 0.0,
                    }
                },
            }
        },
        name="ROI_01_monitor",
        object_keys={"ROI_01": ["ROI_01"]},
    )
    kafka_publisher("descriptor", roi_event_descriptor_doc)

    # copied from run 588bed89-b8e9-4882-86b2-c9471612914e at SRX
    array_counter_event_descriptor_doc, compose_array_counter_event, compose_array_counter_event_page = compose_desc(
        data_keys={
            "ArrayCounter": {
                "source": "PV:XF:08BM-ES{Xsp:1}:C1_ROI1:ArrayCounter_RBV",
                "dtype": "number",
                "shape": [],
                "precision": 4,
                "units": "",
                "lower_ctrl_limit": 0.0,
                "upper_ctrl_limit": 0.0,
            }
        },
        configuration={
            "ArrayCounter": {
                "data": {"ArrayCounter": 0},
                "timestamps": {"ArrayCounter": datetime.datetime.now().timestamp()},
                "data_keys": {
                    "ArrayCounter": {
                        "source": "PV:XF:08BM-ES{Xsp:1}:C1_ROI1:ArrayCounter_RBV",
                        "dtype": "number",
                        "shape": [],
                        "precision": 4,
                        "units": "",
                        "lower_ctrl_limit": 0.0,
                        "upper_ctrl_limit": 0.0,
                    }
                },
            }
        },
        name="array_counter_monitor",
        object_keys={"ArrayCounter": ["ArrayCounter"]},
    )
    kafka_publisher("descriptor", array_counter_event_descriptor_doc)

    for array_counter in range(1, 20):
        time.sleep(1)
        array_counter_event_doc = compose_array_counter_event(
            data={"ArrayCounter": array_counter},
            timestamps={"ArrayCounter": datetime.datetime.now().timestamp()}
        )
        kafka_publisher("event", array_counter_event_doc)
        # make this random?
        roi_event_doc = compose_roi_event(
            data={'xs_channel1_rois_roi1_value': 5.0},
            timestamps={"xs_channel1_rois_roi1_value": datetime.datetime.now().timestamp()},
        )
        kafka_publisher("event", roi_event_doc)

    run_stop_doc = compose_stop()
    kafka_publisher("stop", run_stop_doc)

    kafka_publisher.flush()


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--topic", type=str, default="srx.bluesky.documents")
    argparser.add_argument("--bootstrap-servers", type=str, help="comma-delimited list", default="localhost:9092")

    args = argparser.parse_args()
    print(args)

    send_documents(**vars(args))
