import json

import polars as pl
from polars import Struct, Field, Utf8, List, Float64, last, col, Float32

from controlflow.JoinManager import JoinManager


def test_start():
    join_manager = JoinManager()
    join_manager.settings.rounding_acc = "1ms"
    join_manager.settings.drop_null = False
    join_manager.settings.test_mode = False
    join_manager.settings.out = "../out"
    join_manager.settings.source = (
        "/home/michaelleitner/Documents/contest/Data_TrainTicket/"
    )
    # join_manager.settings.source = (
    #     "/home/michaelleitner/Documents/contest/Data_TrainTicket_Pirmin/"
    # )
    join_manager.settings.output_vis = False
    join_manager.settings.tree_settings.print_data_with_accessing_field = True
    join_manager.settings.tree_settings.accessing_field = 1

    join_manager.settings.visualize_graph = False
    join_manager.settings.neo4j_container_name = "neo4j"
    join_manager.settings.neo4j_uri = "neo4j://localhost:7687"

    join_manager.process()


# schema:
# OrderedDict([('status', Utf8), ('data', Struct([Field('resultType', Utf8), Field('result', List(Struct([Field('metric', Struct([Field('__name__', Utf8), Field('container', Utf8), Field('endpoint', Utf8), Field('id', Utf8), Field('image', Utf8), Field('instance', Utf8), Field('job', Utf8), Field('metrics_path', Utf8), Field('name', Utf8), Field('namespace', Utf8), Field('node', Utf8), Field('pod', Utf8), Field('service', Utf8)])), Field('values', List(List(Utf8)))])))]))])


# schema = {
#     "status": Utf8,
#     "data": Struct(
#         [
#             Field("resultType", Utf8),
#             Field(
#                 "result",
#                 List(
#                     Struct(
#                         [
#                             Field(
#                                 "metric",
#                                 Struct(
#                                     [
#                                         Field("__name__", Utf8),
#                                         Field("container", Utf8),
#                                         Field("endpoint", Utf8),
#                                         Field("id", Utf8),
#                                         Field("image", Utf8),
#                                         Field("instance", Utf8),
#                                         Field("job", Utf8),
#                                         Field("metrics_path", Utf8),
#                                         Field("name", Utf8),
#                                         Field("namespace", Utf8),
#                                         Field("node", Utf8),
#                                         Field("pod", Utf8),
#                                         Field("service", Utf8),
#                                     ]
#                                 ),
#                             ),
#                             Field("values", List(List(Utf8))),
#                         ]
#                     )
#                 ),
#             ),
#         ]
#     ),
# }
#
#
# filepath = "/home/michaelleitner/Documents/contest/Data_TrainTicket/ts-admin-basic-info-service-sprintstarterweb_1.5.22/Monitoring_ts-admin-basic-info-service_springstarterweb_1.5.22.RELEASE.json_2022-07-08/ts-admin-basic-info-service_springstarterweb_1.5.22.RELEASE.json_container_spec_cpu_quota.json"
#
#
# read_json = (
#     pl.read_json(filepath, schema=schema)
#     .unnest("data")
#     .explode("result")
#     .unnest("result")
#     .unnest("metric")
#     .explode("values")
#     .with_columns(
#         [
#             pl.from_epoch(col("values").list[0].cast(Float64)).alias("measure_time"),
#             col("values").list[1].cast(Float64).alias("metric_name"),
#         ]
#     )
#     .drop("values")
# )
# read_json.write_csv("test.csv")
# # read_json = pl.read_json(filepath, schema=schema)
# print(read_json)

if __name__ == "__main__":
    test_start()
    print()
