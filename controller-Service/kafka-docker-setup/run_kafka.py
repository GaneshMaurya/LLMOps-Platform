import socket
import subprocess
import os

def getMyIP():
    local_ip=None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except:
        s.close()
    return local_ip
curr_env=os.environ.copy()

command=['docker','compose','up','-d']
added_env={
    "KAFKA_HOST_IP":getMyIP()
}
curr_env.update(added_env)
try:
    result=subprocess.run(command,env=curr_env,capture_output=True,text=True)
    print('All containers up and running')
except:
    print("Error")

