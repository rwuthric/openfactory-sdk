import os
import time
import csv
from openfactory.apps import OpenFactoryApp
from openfactory.kafka import KSQLDBClient
from openfactory.assets import Asset, AssetAttribute



class ToolMonitoring(OpenFactoryApp):
    """
    ToolMonitoring application for monitoring the state of tools in an IVAC system.
    Inherits from `OpenFactoryApp` and extends it to represent a specific application
    that monitors the state of tools in an IVAC system, updating their conditions based on events.
    Class Attributes:
        IVAC_SYSTEM_UUID (str): Unique identifier for the IVAC system, defaults to 'IVAC'.
    Instance Attributes:
        tool_states (dict): Dictionary to hold the states of tools in the IVAC system.
        ivac (Asset): Asset instance representing the IVAC system.
    """


    IVAC_SYSTEM_UUID: str = os.getenv('IVAC_SYSTEM_UUID', 'IVAC')
    SIMULATION_MODE: str = os.getenv('SIMULATION_MODE', 'false')

    def __init__(self, app_uuid, ksqlClient, bootstrap_servers, loglevel= 'INFO'):
        """
        Initializes the ToolMonitoring application.
        Sets up the application with the provided UUID, KSQLDB client, and Kafka bootstrap servers.
        Args:
            app_uuid (str): Unique identifier for the application.
            ksqlClient: KSQLDB client instance for interacting with KSQLDB.
            bootstrap_servers (str): Comma-separated list of Kafka bootstrap servers.
            loglevel (str): Logging level for the application. Defaults to 'INFO'.
        """
        super().__init__(app_uuid, ksqlClient, bootstrap_servers, loglevel)
        self.tool_states = {}
        self.asset_uuid = "IVAC"

        self.add_attribute('ivac_system', AssetAttribute(
            self.IVAC_SYSTEM_UUID,
            type='Events',
            tag='DeviceUuid'))


        self.ivac= Asset(self.IVAC_SYSTEM_UUID,
                          ksqlClient=ksqlClient,
                          bootstrap_servers=bootstrap_servers)

        self.ivac.add_attribute('ivac_tools_status',
                                AssetAttribute('UNAVAILABLE',
                                               type='Condition',
                                               tag='UNAVAILABLE'))
        
        self.tool_states['A1ToolPlus'] = self.ivac.A1ToolPlus.value
        self.tool_states['A2ToolPlus'] = self.ivac.A2ToolPlus.value

        print(f"Tool states initialized: {self.tool_states}")

        self.setup_power_monitoring_streams(ksqlClient)

        self.method('SimulationMode', self.SIMULATION_MODE)
        print(f'Sent to CMD_STREAM: SimulationMode with value {self.SIMULATION_MODE}')

        ## Initialize buzzer state
        self.verify_tool_states()

        self.ivac.subscribe_to_events(self.on_event, 'ivac_events_group')

        
    def setup_power_monitoring_streams(self, ksqlClient: KSQLDBClient) -> None:
        """
        Setup KSQL streams for monitoring power events and durations.
        Creates the necessary streams and tables for power state monitoring.
        """
        try:
            #First, cleanup any existing streams and tables
            ksqlClient.statement_query("DROP TABLE IF EXISTS ivac_power_state_totals;")
            ksqlClient.statement_query("DROP STREAM IF EXISTS ivac_power_durations;")
            ksqlClient.statement_query("DROP TABLE IF EXISTS latest_ivac_power_state;")
            ksqlClient.statement_query("DROP STREAM IF EXISTS ivac_power_events;")
            
            print("Cleaned up existing streams and tables for power monitoring.")

            # Create the power events stream
            power_events_query = """
            CREATE STREAM IF NOT EXISTS ivac_power_events WITH (KAFKA_TOPIC='power_events', PARTITIONS=1) AS
            SELECT
              id AS key,
              asset_uuid,
              value,
              ROWTIME AS ts
            FROM ASSETS_STREAM
            WHERE asset_uuid = 'IVAC' AND id IN ('A1ToolPlus', 'A2ToolPlus')
            EMIT CHANGES;
            """
            
            # Create the latest state table
            latest_state_query = """
            CREATE TABLE IF NOT EXISTS latest_ivac_power_state AS
            SELECT
              key,
              LATEST_BY_OFFSET(value) AS last_value,
              LATEST_BY_OFFSET(ts) AS last_ts
            FROM ivac_power_events
            GROUP BY key;
            """
            
            # Create the power durations stream
            durations_query = """
            CREATE STREAM IF NOT EXISTS ivac_power_durations AS
            SELECT
              ivac_event.key,
              s.last_value AS state_just_ended,
              (ivac_event.ts - s.last_ts) / 1000 AS duration_sec
            FROM ivac_power_events ivac_event
            JOIN latest_ivac_power_state s
              ON ivac_event.key = s.key
            WHERE ivac_event.value IS DISTINCT FROM s.last_value
            EMIT CHANGES;
            """
            
            # Create the totals table
            totals_query = """
            CREATE TABLE IF NOT EXISTS ivac_power_state_totals AS
            SELECT
              CONCAT(IVAC_EVENT_KEY, '_', STATE_JUST_ENDED) AS ivac_power_key,
              SUM(DURATION_SEC) AS total_duration_sec,
              COUNT(*) AS state_change_count
            FROM ivac_power_durations
            GROUP BY CONCAT(IVAC_EVENT_KEY, '_', STATE_JUST_ENDED)
            EMIT CHANGES;
            """

            # Execute the queries
            queries = [
                ("Power Events Stream", power_events_query),
                ("Latest State Table", latest_state_query),
                ("Power Durations Stream", durations_query),
                ("Power Totals Table", totals_query)
            ]

            for name, query in queries:
                try:
                    response = ksqlClient.statement_query(query)
                    print(f"Created {name}: {response}")
                except Exception as e:
                    print(f"Error creating {name}: {e}")
                
        except Exception as e:
            print(f"KSQL setup error: {e}")

    def app_event_loop_stopped(self) -> None:
        """
        Called automatically when the main application event loop is stopped.

        This method handles cleanup tasks such as stopping the temperature
        sensor's sample subscription to ensure a graceful shutdown.
        """
        print("Stopping Temperature sensor consumer thread ...")
        self.ivac.stop_events_subscription()

    def main_loop(self) -> None:
        """ Main loop of the App. """
        while True:
            time.sleep(1)

    
    def on_event(self, msg_key:str, msg_value:dict) -> None:
        """
        Callback for handling new events from the ivac system.

        Verifies the state of the tools of the ivac system and updates
        the ivac_condition attribute based on these conditions:
        - If all tools are in 'ON' state, sets ivac_condition to 'ERROR'..
        - If any tool is in 'UNAVAILABLE' state, sets ivac_condition to 'UNAVAILABLE'.
        - Else, sets ivac_condition to 'OK'.

        Writes the event data to a CSV file.

        Args:
            msg_key (str): The key of the Kafka message (the sensor ID).
            msg_value (dict): The message payload containing sample data.
                              Expected keys: 'id' (str), 'value' (float or str).
        """
        if(msg_value['id'] == 'A1ToolPlus'):
             self.tool_states['A1ToolPlus'] = msg_value['value']
        elif(msg_value['id'] == 'A2ToolPlus'):
            self.tool_states['A2ToolPlus'] = msg_value['value']
            
        self.verify_tool_states()
        
        self.write_message_to_csv(msg_key, msg_value)

    def verify_tool_states(self) -> None:
        """
        Verifies the state of the tools and updates the ivac_condition attribute.

        This method checks the states of the tools in the ivac system and updates
        the ivac_condition attribute based on the following conditions:
        - If all tools are in 'ON' state, sets ivac_condition to 'ERROR'.
        - If any tool is in 'UNAVAILABLE' state, sets ivac_condition to 'UNAVAILABLE'.
        - Else, sets ivac_condition to 'OK'.

        Sends the updated ivac_condition to the CMDS_STREAM to control the buzzer.

        Args:
            tool_states (dict): A dictionary containing tool states with tool IDs as keys.
        """
        print(f"Current tool states: {self.tool_states.values()}")

        if any(state == 'OFF' for state in self.tool_states.values()):
            self.ivac.add_attribute('ivac_tools_status',
                                    AssetAttribute('No more than one connected tool is powered ON',
                                                   type='Condition',
                                                   tag='NORMAL')) 
        elif any(state == 'UNAVAILABLE' for state in self.tool_states.values()):
            self.ivac.add_attribute('ivac_tools_status',
                                    AssetAttribute('At least one tool is UNAVAILABLE',
                                                   type='Condition',
                                                   tag='WARNING')) 
        else:
            self.ivac.add_attribute('ivac_tools_status',
                                    AssetAttribute('More than one connected tool is powered ON.',
                                                   type='Condition',
                                                   tag='FAULT')) 

        time.sleep(0.5)  # Ensure that ivac_tools_status is set before sending
        self.method("BuzzerControl", self.ivac.__getattr__('ivac_tools_status').tag)
        print(f'Sent to CMD_STREAM: BuzzerControl with value {self.ivac.__getattr__('ivac_tools_status').value}')

    def write_message_to_csv(self, msg_key: str, msg_value: dict) -> None:
        """
        Writes the relevant message data to a CSV file named '${DEVICE_UUID}_msgs.csv'.

        This method appends the event data to the CSV file, creating the file
        if it does not exist. It ensures that the header is written only once.

        Args:
            msg_key (str): The key of the Kafka message (the sensor ID).
            msg_value (dict): The message payload containing sample data.
                              Expected keys: 'id' (str), 'value' (float or str).
        """
        with open(f'{msg_key}_msgs.csv', 'a', newline='') as csvfile:
            fieldnames = list(msg_value.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            csvfile.seek(0, 2)
            if csvfile.tell() == 0:
                writer.writeheader()

            writer.writerow(msg_value)
       


app = ToolMonitoring(
    app_uuid='TOOL-MONITORING',
    ksqlClient=KSQLDBClient("http://ksqldb-server:8088"),
    bootstrap_servers="broker:29092"
)
app.run()

