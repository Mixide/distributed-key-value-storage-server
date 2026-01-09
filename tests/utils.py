import logging
import grpc
import socket
import shutil
        
from concurrent import futures
from protos import mapb_pb2 as mapb
from protos import stpb_pb2_grpc as stpb_grpc
from storage.main import StoreService

def _get_free_port():
    """向 OS 申请一个可用端口"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]
    
def _start_storage(manager_api):
    fakelogger = logging.getLogger("storage")
    fakelogger.handlers.clear()  
    port = ":"+ str(_get_free_port())
    storage_service = StoreService(ip='localhost', port=port, cache_num=5, manager_addr=manager_api)
    storage_service.start('tests/', fakelogger)
    shutil.rmtree(storage_service.datapath)
        
    return storage_service, f"localhost{port}", storage_service.token