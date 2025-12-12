import grpc
import signal
import sys
import time

from protos import mapb_pb2 as mapb
from protos import mapb_pb2_grpc as mapb_grpc
from protos import stpb_pb2 as stpb
from protos import stpb_pb2_grpc as stpb_grpc
from params import params


def reconnect(ma_stub, client_id: int):
    for _ in range(10):
        try:
            resp = ma_stub.changeServerRandom(mapb.CliId(cli_id=client_id))
            if resp.errno:
                api = resp.api
                if not api:
                    continue
                # api expected to be like 'host:port'
                return grpc.insecure_channel(api)
        except Exception:
            # ignore and retry
            time.sleep(0.2)
            continue
    raise RuntimeError("无法连接至服务器")

def shell(ma_stub, ma_chan, st_stub, st_chan, client_id):
    def handle_sig(signum, frame):
        print('接收到中断信号，正在退出...')
        try:
            ma_stub.disconnect(mapb.CliId(cli_id=client_id))
            ma_chan.close()
        except Exception:
            pass
        try:
            st_chan.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    print("开始输入命令")
    while True:
        try:
            line = input('>>> ').strip()
        except EOFError:
            break
        if not line:
            continue
        args = line.split()
        cmd = args[0].upper()

        if cmd == 'EXIT':
            break
        if cmd == 'HELP':
            print('使用 get [key] 来获取key对应的键值')
            print('使用 put [key] [value] 来上传键值对')
            print('使用 del [key] 来删除key对应的键值')
            print('使用 change <api> 更改存储服务器, 不指定api时随机分配')
            print('使用 exit 结束运行')
            continue

        def call_with_reconnect(rpc_func, request):
            nonlocal st_chan, st_stub
            try:
                return rpc_func(request)
            except Exception as e:
                # try reconnect
                try:
                    try:
                        st_chan.close()
                    except Exception:
                        pass
                    st_chan = reconnect(ma_stub, client_id)
                    st_stub = stpb_grpc.storagementServiceStub(st_chan)
                except Exception as re:
                    raise re
            # if still failed, raise
            try:
                return rpc_func(request)
            except Exception:
                raise RuntimeError('RPC failed after reconnect')

        try:
            if cmd == 'GET':
                if len(args) != 2:
                    print('不正确的参数个数')
                    continue
                key = args[1]
                resp = call_with_reconnect(lambda r: st_stub.getdata(r), stpb.StRequest(cli_id=client_id, key=key))
                if not resp.errno:
                    print(resp.errmes)
                else:
                    print(resp.value)

            elif cmd == 'PUT':
                if len(args) != 3:
                    print('不正确的参数个数')
                    continue
                key, value = args[1], args[2]
                resp = call_with_reconnect(lambda r: st_stub.putdata(r), stpb.StKV(cli_id=client_id, key=key, value=value))
                if not resp.errno:
                    print(resp.errmes)
                else:
                    print('上传成功')

            elif cmd == 'DEL':
                if len(args) != 2:
                    print('不正确的参数个数')
                    continue
                key = args[1]
                resp = call_with_reconnect(lambda r: st_stub.deldata(r), stpb.StRequest(cli_id=client_id, key=key))
                if not resp.errno:
                    print(resp.errmes)
                else:
                    print('删除成功')

            elif cmd == 'CHANGE':
                if len(args) == 1:
                    # random change
                    resp = ma_stub.changeServerRandom(mapb.CliId(cli_id=client_id))
                    if not resp.errno:
                        print(resp.errmes)
                    else:
                        api = resp.api
                        st_chan.close()
                        st_chan = grpc.insecure_channel(api)
                        st_stub = stpb_grpc.storagementServiceStub(st_chan)
                        print('切换成功')
                elif len(args) == 2:
                    api = args[1]
                    resp = ma_stub.changeServer(mapb.CliChange(cli_id=client_id, api=api))
                    if not resp.errno:
                        print(resp.errmes)
                    else:
                        st_chan.close()
                        st_chan = grpc.insecure_channel(api)
                        st_stub = stpb_grpc.storagementServiceStub(st_chan)
                        print('切换成功')
                else:
                    print('不正确的参数个数')

            else:
                print('无效命令')

        except Exception as e:
            print('发生错误:', e)

def main():
    manage_target = params.MANAGER_IP + params.MANAGER_PORT
    ma_chan = grpc.insecure_channel(manage_target)
    ma_stub = mapb_grpc.manageServiceStub(ma_chan)
    try:
        info = ma_stub.connect(mapb.Empty())
    except Exception as e:
        print('连接管理服务器时发生错误:', e)
        return
    finally:
        ma_chan.close()

    if not info.errno:
        print(info.errmes)
        return

    client_id = info.cli_id
    print(f"已连接至管理服务器, 客户端ID为 {client_id}")
    ip = info.ip
    port = info.port

    storage_target = ip + port
    print(f"连接至存储服务器 {storage_target}")
    st_chan = grpc.insecure_channel(storage_target)
    st_stub = stpb_grpc.storagementServiceStub(st_chan)

    shell(ma_stub, ma_chan, st_stub, st_chan, client_id)

    try:
        ma_stub.disconnect(mapb.CliId(cli_id=client_id))
        ma_chan.close()
    except Exception:
        pass

    try:
        st_chan.close()
    except Exception:
        pass


if __name__ == '__main__':
    main()
