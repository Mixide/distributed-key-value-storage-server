import logging
import random

from tests.utils import _start_storage
from protos import mapb_pb2 as mapb
from server.main import ManageService

def test_node_register_and_unregister(manager_server):
    manager_stub, manage_service, _ = manager_server
    dummy_storage_info = mapb.SerRequest(ip="localhost", port="50051",token="0")
    server_info = manager_stub.online(dummy_storage_info, None)
    sid = server_info.server_id
    assert sid in manage_service.servermap
    manager_stub.offline(mapb.SerInfo(server_id = sid),None)
    assert sid not in manage_service.servermap

def test_check_all_storage_live(manager_server):
    _, manage_service, manager_api = manager_server
    # 启动一个假的 storage server 并注册
    _start_storage(manager_api)
    _start_storage(manager_api)
    _start_storage(manager_api)

    manage_service.check_all_storage_live()
    assert True

def test_change_store_server(manager_server, storage_server):
    manager_stub, _, manager_api = manager_server
    _, api0, _ = storage_server
    clinent_info = manager_stub.connect(mapb.Empty())
    assert clinent_info.ip + clinent_info.port == api0

    client_id = clinent_info.cli_id
    
    # 启动一个假的 storage server 并注册
    _, api1, _ = _start_storage(manager_api)
    _, api2, _ = _start_storage(manager_api)

    resp = manager_stub.changeServer(mapb.CliId(cli_id=client_id))
    assert resp.errno
    assert resp.api in [api0, api1, api2]

def test_verify(manager_server):
    manager_stub, _, _ = manager_server
    key = "testkey"
    value = "testvalue"

    fake_sid = random.randint(1, 2**31-1)
    resp = manager_stub.Put(mapb.KV(server_id = fake_sid, key=key, value=value))
    assert not resp.errno and resp.errmes == "节点未注册, 无权操作!"

    resp = manager_stub.Get(mapb.Request(server_id = fake_sid, key=key))
    assert not resp.errno and resp.errmes == "节点未注册, 无权操作!"

    resp = manager_stub.Del(mapb.Request(server_id = fake_sid, key=key))
    assert not resp.errno and resp.errmes == "节点未注册, 无权操作!"