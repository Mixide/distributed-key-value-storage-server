# manage_service.py — full Python implementation converted from main.go
# Requires generated protobuf modules: mapb_pb2, mapb_pb2_grpc, stpb_pb2, stpb_pb2_grpc
# Also expects a params.py with Manageport defined (e.g. ":50051").

import logging
import random
import time
import grpc
from concurrent import futures
from threading import Lock

from protos import mapb_pb2 as mapb
from protos import mapb_pb2_grpc as mapb_grpc
from protos import stpb_pb2 as stpb
from protos import stpb_pb2_grpc as stpb_grpc
from params import params

class SerNode:
    def __init__(self, host: str, port: str, sid: int):
        self.host = host
        self.port = port
        self.id = sid

class ManageService(mapb_grpc.manageServiceServicer):
    def __init__(self, logger: logging.Logger):
        self.servermap: dict[int, SerNode] = {}
        self.clientmap: dict[int, str] = {}
        self.APImap: dict[str, bool] = {}
        self.logger = logger
        self.mu = Lock()

    def _rand_id(self) -> int:
        # returns a positive 32-bit int
        return random.randint(1, 2**31-1)

    def getServerId(self) -> int:
        sid = self._rand_id()
        while sid in self.servermap:
            sid = self._rand_id()
        return sid

    def getClientId(self) -> int:
        cid = self._rand_id()
        while cid in self.clientmap:
            cid = self._rand_id()
        return cid

    def getServerInfo(self) -> tuple[str, str]:
        if not self.servermap:
            raise RuntimeError("No servers available")
        node = random.choice(list(self.servermap.values()))
        return node.host, node.port

    # ----- RPC implementations (translate one-to-one from Go) -----
    def changeServer(self, request: mapb.CliChange, context) -> mapb.Empty:
        API = request.api 
        cli_id = request.cli_id
        self.logger.info(f"客户端{cli_id} 试图更换服务器为{API}")
        if API not in self.APImap:
            self.logger.info(f"无法为客户端{cli_id} 更换服务器为{API}, 保持连接{self.clientmap.get(cli_id)}")
            return mapb.Empty(errno=False, errmes="不存在此存储服务器")
        self.clientmap[cli_id] = API
        self.logger.info(f"成功为客户端{cli_id} 更换连接服务器为{API}")
        return mapb.Empty(errno=True)

    def changeServerRandom(self, request: mapb.CliId, context) -> mapb.ChangeInfo:
        if len(self.servermap) == 0:
            self.logger.info("客户端试图更换连接, 但目前暂无键值存储服务器")
            return mapb.ChangeInfo(errno=False, errmes="连接失败, 目前暂无键值服务器")
        host, port = self.getServerInfo()
        cli_id = request.cli_id
        self.clientmap[cli_id] = host + port
        self.logger.info(f"成功为客户端{cli_id} 更换连接服务器为{host+port}")
        return mapb.ChangeInfo(api=host+port, errno=True)

    def connect(self, request: mapb.Empty, context) -> mapb.CliInfo:
        if len(self.servermap) == 0:
            self.logger.info("客户端试图连接, 但目前暂无键值存储服务器")
            return mapb.CliInfo(errno=False, errmes="连接失败, 目前暂无键值服务器")
        host, port = self.getServerInfo()
        cid = self.getClientId()
        self.clientmap[cid] = host + port
        self.logger.info(f"客户端连接{self.clientmap[cid]}, 为其分配id: {cid}")
        return mapb.CliInfo(host=host, port=port, cli_id=cid, errno=True)

    def online(self, request: mapb.SerRequest, context) -> mapb.SerInfo:
        host = request.host
        port = request.port
        sid = self.getServerId()
        self.servermap[sid] = SerNode(host=host, port=port, sid=sid)
        self.APImap[host+port] = True
        self.logger.info(f"存储服务器 {host}{port} 注册 分配id为: {sid}")
        return mapb.SerInfo(server_id=sid, errno=True)

    def offline(self, request: mapb.SerInfo, context) -> mapb.Empty:
        sid = request.server_id if hasattr(request, 'server_id') else getattr(request, 'serverId', 0)
        node = self.servermap.get(sid)
        if node:
            host, port = node.host, node.port
            del self.servermap[sid]
            self.APImap.pop(host+port, None)
            self.logger.info(f"存储服务器 {host}{port} 注消")
        return mapb.Empty(errno=True)

    def Get(self, request: mapb.Request, context) -> mapb.Response:
        ser_id = request.server_id if hasattr(request, 'server_id') else getattr(request, 'serverId', 0)
        key = request.key
        self.logger.info(f"存储服务器{ser_id} 请求键值{key}")
        values = []
        self.logger.info(f"正在从其他存储服务器收集键值{key}")
        for sid, ser in self.servermap.items():
            if sid == ser_id:
                continue
            host, port = ser.host, ser.port
            target = host + port
            self.logger.info(f"向存储服务器{sid} 请求键值{key}")
            try:
                with grpc.insecure_channel(target) as ch:
                    client = stpb_grpc.storagementServiceStub(ch)
                    resp = client.getdata(stpb.StRequest(cli_id=0, key=key))
            except Exception as e:
                self.logger.error(e)
                continue
            if not getattr(resp, 'errno', getattr(resp, 'Errno', False)):
                self.logger.info(f"无法从存储服务器{sid} 获取键值{key} ,{getattr(resp, 'errmes', getattr(resp, 'Errmes',''))}")
                continue
            else:
                self.logger.info(f"存储服务器{sid} 响应了键值{key} 请求")
            values.append(resp.value)
        maxnum = len(values)
        if maxnum == 0:
            return mapb.Response(errno=False, errmes=f"暂时缺少键值{key}")
        self.logger.info(f"从存储服务器中共收集{maxnum}个键值{key},检测一致性")
        count_map: dict[str, int] = {}
        find_value = ""
        cnt = 0
        for v in values:
            count_map[v] = count_map.get(v, 0) + 1
            if count_map[v] > cnt:
                find_value = v
                cnt = count_map[v]
        if cnt > maxnum // 2:
            self.logger.info(f"键值{key} 达成一致")
            return mapb.Response(value=find_value, errno=True)
        self.logger.info(f"键值{key} 未能达成一致")
        return mapb.Response(errno=False, errmes=f"其他服务器对键值{key} 无法达成一致")

    def Put(self, request: mapb.KV, context) -> mapb.Response:
        self.mu.acquire()
        try:
            key = request.key
            value = request.value
            ser_id = request.server_id if hasattr(request, 'server_id') else getattr(request, 'serverId', 0)
            self.logger.info(f"存储服务器{ser_id} 申请提交键值{key}")
            hasprc: dict[int, str] = {}
            flag = True
            for sid, ser in self.servermap.items():
                host, port = ser.host, ser.port
                target = host + port
                self.logger.info(f"向存储服务器{sid} 广播键值{key} 提交")
                try:
                    with grpc.insecure_channel(target) as ch:
                        client = stpb_grpc.storagementServiceStub(ch)
                        errno = client.maPutdata(stpb.StKV(key=key, value=value))
                except Exception as e:
                    self.logger.error(e)
                    continue
                if not getattr(errno, 'errno', getattr(errno, 'Errno', False)):
                    self.logger.info(f"存储服务器{sid} 拒绝写入键值{key}, {getattr(errno,'errmes',getattr(errno,'Errmes',''))}")
                    flag = False
                else:
                    self.logger.info(f"存储服务器{sid} 同意写入键值{key}, {getattr(errno,'errmes',getattr(errno,'Errmes',''))}")
                hasprc[sid] = target
            if flag:
                self.logger.info(f"存储服务器达成共识, 写入本次键值{key}")
                for target in hasprc.values():
                    try:
                        with grpc.insecure_channel(target) as ch:
                            client = stpb_grpc.storagementServiceStub(ch)
                            client.commit(stpb.StRequest(key=key, delete=False))
                    except Exception as e:
                        self.logger.error(e)
                        continue
            else:
                self.logger.info(f"存储服务器未达成共识, 拒绝写入键值{key}")
                for target in hasprc.values():
                    try:
                        with grpc.insecure_channel(target) as ch:
                            client = stpb_grpc.storagementServiceStub(ch)
                            client.abort(stpb.StRequest(key=key, delete=False))
                    except Exception as e:
                        self.logger.error(e)
                        continue
                self.logger.info(f"本次键值{key} 提交无效")
                return mapb.Response(errno=False, errmes="提交失败")
            self.logger.info(f"本次键值{key} 提交生效")
            return mapb.Response(errno=True)
        finally:
            self.mu.release()

    def Del(self, request: mapb.Request, context) -> mapb.Response:
        self.mu.acquire()
        try:
            key = request.key
            ser_id = request.server_id if hasattr(request, 'server_id') else getattr(request, 'serverId', 0)
            self.logger.info(f"存储服务器{ser_id} 申请删除键值{key}")
            hasprc: dict[int, str] = {}
            flag = True
            for sid, ser in self.servermap.items():
                host, port = ser.host, ser.port
                target = host + port
                self.logger.info(f"向存储服务器{sid} 广播键值{key} 删除")
                try:
                    with grpc.insecure_channel(target) as ch:
                        client = stpb_grpc.storagementServiceStub(ch)
                        errno = client.maDeldata(stpb.StRequest(key=key))
                except Exception as e:
                    self.logger.error(e)
                    continue
                if not getattr(errno, 'errno', getattr(errno, 'Errno', False)):
                    self.logger.info(f"存储服务器{sid} 拒绝删除键值{key}, {getattr(errno,'errmes',getattr(errno,'Errmes',''))}")
                    flag = False
                else:
                    self.logger.info(f"存储服务器{sid} 同意删除键值{key}, {getattr(errno,'errmes',getattr(errno,'Errmes',''))}")
                hasprc[sid] = target
            if flag:
                self.logger.info(f"存储服务器达成共识,删除键值{key}")
                for target in hasprc.values():
                    try:
                        with grpc.insecure_channel(target) as ch:
                            client = stpb_grpc.storagementServiceStub(ch)
                            client.commit(stpb.StRequest(key=key, delete=True))
                    except Exception as e:
                        self.logger.error(e)
                        continue
            else:
                self.logger.info(f"存储服务器未达成共识, 拒绝删除键值{key}")
                for target in hasprc.values():
                    try:
                        with grpc.insecure_channel(target) as ch:
                            client = stpb_grpc.storagementServiceStub(ch)
                            client.abort(stpb.StRequest(key=key, delete=True))
                    except Exception as e:
                        self.logger.error(e)
                        continue
                self.logger.info(f"本次键值{key} 删除无效")
                return mapb.Response(errno=False, errmes="删除失败")
            self.logger.info(f"本次键值{key} 删除生效")
            return mapb.Response(errno=True)
        finally:
            self.mu.release()

    def disconnect(self, request: mapb.CliId, context) -> mapb.Empty:
        cid = request.cli_id if hasattr(request, 'cli_id') else getattr(request, 'cliId', 0)
        self.logger.info(f"客户端{cid} 申请退出连接")
        self.clientmap.pop(cid, None)
        self.logger.info(f"客户端{cid} 成功退出")
        return mapb.Empty(errno=True)


def serve():
    logger = logging.getLogger("manage")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("server/manage.log", mode='w', encoding='utf-8')
    fh.setFormatter(logging.Formatter("[Info] - %(message)s"))
    logger.addHandler(fh)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    service = ManageService(logger)
    mapb_grpc.add_manageServiceServicer_to_server(service, server)
    server.add_insecure_port(getattr(params, 'MANAGER_IP', ':50051'))

    logger.info("开始进行服务")
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("接收到中断信号, 退出服务")
        server.stop(0)


if __name__ == '__main__':
    serve()
