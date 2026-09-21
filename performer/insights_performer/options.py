import json
from typing import Any

from couchbase_operational_insights.common.deserializer import (
    DefaultJsonDeserializer,
    Deserializer,
    PassthroughDeserializer,
)
from insights_performer.exceptions import PerformerException
from insights_performer.protocol.columnar import serialization_pb2 as serialization_pb


class CustomJsonDeserializer(Deserializer):
    def deserialize(self, value: bytes) -> Any:
        res = json.loads(value.decode('utf-8'))
        if not isinstance(res, dict):
            raise RuntimeError('CustomJsonDeserializer only supports rows that are JSON objects')
        res['Serialized'] = False
        return res


class SharedOptions:
    @staticmethod
    def convert_deserializer(proto_deserializer: serialization_pb.Deserializer) -> Deserializer:
        deserializer_type = proto_deserializer.WhichOneof('type')
        if proto_deserializer.HasField('json'):
            return DefaultJsonDeserializer()
        elif proto_deserializer.HasField('passthrough'):
            return PassthroughDeserializer()
        elif proto_deserializer.HasField('custom'):
            return CustomJsonDeserializer()
        raise PerformerException(f'Deserializer type not supported: {deserializer_type}')
