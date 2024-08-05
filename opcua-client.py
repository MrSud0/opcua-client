import asyncio
from asyncua import Client
from asyncua.ua import VariantType
import logging
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("opcua_client")

async def convert_value_to_node_type(node, value):
    # Read the data type of the node
    data_type = await node.read_data_type_as_variant_type()
    
    # Convert the value based on the data type
    if data_type in [VariantType.Int16, VariantType.Int32, VariantType.Int64]:
        return int(value)
    elif data_type in [VariantType.Float, VariantType.Double]:
        return float(value)
    elif data_type == VariantType.Boolean:
        return bool(int(value))  # Convert 0 or 1 to False or True
    elif data_type == VariantType.String:
        return str(value)
    else:
        raise ValueError(f"Unsupported data type: {data_type}")

async def main(opc_ua_server_url, auth_type, username, password, operation, node_id, value=None):
    client = Client(opc_ua_server_url)

    if auth_type == "userpass":
        if username and password:
            client.set_user(username)
            client.set_password(password)
        else:
            log.error("Username and password must be provided for userpass authentication")
            return
    elif auth_type != "anonymous":
        log.error("Invalid authentication type. Exiting.")
        return

    try:
        await client.connect()
        log.info("Connected to OPC UA server")

        if operation == "read":
            node = client.get_node(node_id)
            value = await node.read_value()
            log.info(f"Value of node {node_id}: {value}")
        elif operation == "write":
            if value is None:
                log.error("Value must be provided for write operation")
                return
            node = client.get_node(node_id)
            try:
                converted_value = await convert_value_to_node_type(node, value)
                log.info(f"Attempting to write value {converted_value} to node {node_id}")
                await node.write_value(converted_value)
                log.info(f"Updated value of node {node_id} to {converted_value}")
                
                # Read back the value to verify the write
                new_value = await node.read_value()
                if new_value == converted_value:
                    log.info(f"Verified: Value of node {node_id} is now {new_value}")
                else:
                    log.warning(f"Verification failed: Expected {converted_value}, but got {new_value}")
            except ValueError as ve:
                log.error(f"Value conversion error: {ve}")
            except Exception as e:
                log.error(f"An error occurred while writing to the node: {e}")
        else:
            log.error("Invalid operation. Please enter 'read' or 'write'.")

    except Exception as e:
        log.error(f"An error occurred: {e}")
    finally:
        await client.disconnect()
        log.info("Disconnected from OPC UA server")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='OPC UA Client Script')
    parser.add_argument('--hostname', type=str, default='localhost', help='Hostname for the OPC UA server')
    parser.add_argument('--port', type=int, default=4840, help='Port for the OPC UA server')
    parser.add_argument('--path', type=str, default='/freeopcua/server/', help='Path for the OPC UA server endpoint')
    parser.add_argument('--auth-type', type=str, choices=['anonymous', 'userpass'], required=True, help='Authentication type for the OPC UA server')
    parser.add_argument('--username', type=str, help='Username for authentication')
    parser.add_argument('--password', type=str, help='Password for authentication')
    parser.add_argument('--operation', type=str, choices=['read', 'write'], required=True, help='Operation to perform (read/write)')
    parser.add_argument('--node-id', type=str, required=True, help='Node ID to operate on (e.g., ns=2;i=2003)')
    parser.add_argument('--value', type=str, help='Value to write (required for write operation)')

    args = parser.parse_args()
    opc_ua_server_url = f"opc.tcp://{args.hostname}:{args.port}{args.path}"

    asyncio.run(main(opc_ua_server_url, args.auth_type, args.username, args.password, args.operation, args.node_id, args.value))
