import logging
import grpc

from concurrent import futures
from protos import mapb_pb2 as mapb
from protos import stpb_pb2_grpc as stpb_grpc
from storage.main import StoreService

def _start_storage(manager_stub, manager_api):
    fakelogger = logging.getLogger("storage")
    fakelogger.handlers.clear()   # 移除所有 handler    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    port = ":"+str(server.add_insecure_port("localhost:0"))
    storage_service = StoreService(cache_num=5, manager_addr=manager_api)
    info = manager_stub.online(mapb.SerRequest(ip="localhost", port=str(port), token="0"))
    storage_service.logger, storage_service.token, storage_service.id = fakelogger, "0", info.server_id
    
    stpb_grpc.add_storagementServiceServicer_to_server(storage_service, server)
    
    server.start()
    return storage_service, f"localhost{port}"