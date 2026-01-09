import pytest
import grpc
import logging
import os

from concurrent import futures


from protos import mapb_pb2_grpc as mapb_grpc
from protos import mapb_pb2 as mapb
from protos import stpb_pb2_grpc as stpb_grpc


from server.main import ManageService
from storage.main import StoreService
from tests.utils import _get_free_port


@pytest.fixture(scope="function")
def manager_server():
    savepath = "tests/manage/"
    test_ip = 'localhost'
    test_port = ':' + str(_get_free_port())
    manage_service = ManageService(test_ip, test_port)
    manage_service.start(savepath)
    channel = grpc.insecure_channel(test_ip+test_port)
    manager_stub = mapb_grpc.manageServiceStub(channel)

    yield manager_stub, manage_service, test_ip+test_port

    try:
        channel.close()
        manage_service.server.stop(None).wait()
        for handler in manage_service.logger.handlers[:]:
            handler.close()           
            manage_service.logger.removeHandler(handler)
        import shutil
        shutil.rmtree(savepath)
    except Exception:
        pass
    




@pytest.fixture(scope="function")
def storage_server(manager_server):
    manager_stub, _, manager_addr = manager_server
    test_ip = 'localhost'
    test_port = ':' + str(_get_free_port())
    storage_service = StoreService(test_ip, test_port, cache_num=5, manager_addr=manager_addr)
    storage_service.start('tests/')
    store_channel = grpc.insecure_channel(f"localhost{test_port}")
    store_stub = stpb_grpc.storagementServiceStub(store_channel)
    yield store_stub, test_ip+test_port, storage_service.token
    try:
        storage_service.unregister()
        store_channel.close()
        storage_service.server.stop(None).wait()
        storage_service.clean()
    except Exception as e:
        print(e, flush=True)