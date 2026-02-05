from opcua import Server
from opcua import ua
import random
import time

# Create OPC UA Server
server = Server()

# IMPORTANT: set open (no-security) endpoint
server.set_endpoint("opc.tcp://0.0.0.0:4841")

# Disable security completely (THIS FIXES YOUR ERROR)
server.set_security_policy([
    ua.SecurityPolicyType.NoSecurity
])

# Register namespace
uri = "http://example.opcua.server"
idx = server.register_namespace(uri)

# Create object and tags
objects = server.get_objects_node()
myobj = objects.add_object(idx, "DummyData")

tags = []
for i in range(1, 11):
    tag = myobj.add_variable(idx, f"Tag{i}", 0)
    tag.set_writable()
    tags.append(tag)

# Start server
server.start()
print("OPC UA Server started at opc.tcp://localhost:4841 (No Security)")

try:
    while True:
        for tag in tags:
            tag.set_value(round(random.uniform(0, 100), 2))
        time.sleep(1)
except KeyboardInterrupt:
    server.stop()
