from pymavlink import mavutil
import time
import logging
import threading
import queue
import math
from typing import Optional, List, Dict, Union, Any

class MAVLinkHandlerCore:
    """
    Manages the MAVLink connection, message handling, and commands in a thread.
    Includes message cleaning (NaN->None), mission download, mission upload logic,
    and basic vehicle state tracking.
    Designed to be a base class for specific vehicle types (Copter, Plane, etc.).
    """
    # --- MAVLink Constants ---
    MISSION_TYPE = mavutil.mavlink.MAV_MISSION_TYPE_MISSION
    MISSION_ITEM_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_MISSION_ITEM_INT
    MISSION_REQUEST_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_MISSION_REQUEST_INT
    MISSION_COUNT_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_MISSION_COUNT
    MISSION_ACK_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_MISSION_ACK
    COMMAND_ACK_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_COMMAND_ACK
    HEARTBEAT_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_HEARTBEAT
    GLOBAL_POSITION_INT_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_GLOBAL_POSITION_INT
    BATTERY_STATUS_TYPE = mavutil.mavlink.MAVLINK_MSG_ID_BATTERY_STATUS
    
    ATTITUDE_TYPE_ID = mavutil.mavlink.MAVLINK_MSG_ID_ATTITUDE
    LOCAL_POSITION_NED_TYPE_ID = mavutil.mavlink.MAVLINK_MSG_ID_LOCAL_POSITION_NED
    GPS_RAW_INT_TYPE_ID = mavutil.mavlink.MAVLINK_MSG_ID_GPS_RAW_INT
    VFR_HUD_TYPE_ID = mavutil.mavlink.MAVLINK_MSG_ID_VFR_HUD
    EXTENDED_SYS_STATE_TYPE_ID = mavutil.mavlink.MAVLINK_MSG_ID_EXTENDED_SYS_STATE
    SYS_STATUS_TYPE_ID=mavutil.mavlink.MAVLINK_MSG_ID_SYS_STATUS
    STATUSTEXT_TYPE_ID = mavutil.mavlink.MAVLINK_MSG_ID_STATUSTEXT

    MISSION_ACCEPTED = mavutil.mavlink.MAV_MISSION_ACCEPTED
    MISSION_ERROR = mavutil.mavlink.MAV_MISSION_ERROR

    CMD_ACCEPTED = mavutil.mavlink.MAV_RESULT_ACCEPTED
    CMD_FAILED = mavutil.mavlink.MAV_RESULT_FAILED
    CMD_SET_MESSAGE_INTERVAL = mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL

    MAV_MODE_FLAG_SAFETY_ARMED = mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED # 128
    MAV_MODE_FLAG_CUSTOM_MODE_ENABLED = mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED # 1
    
    MAV_PARAM_TYPE_REAL32 = mavutil.mavlink.MAV_PARAM_TYPE_REAL32
    MAV_PARAM_TYPE_UINT8 = mavutil.mavlink.MAV_PARAM_TYPE_UINT8
    MAV_PARAM_TYPE_INT8 = mavutil.mavlink.MAV_PARAM_TYPE_INT8
    MAV_PARAM_TYPE_UINT16 = mavutil.mavlink.MAV_PARAM_TYPE_UINT16
    MAV_PARAM_TYPE_INT16 = mavutil.mavlink.MAV_PARAM_TYPE_INT16
    MAV_PARAM_TYPE_UINT32 = mavutil.mavlink.MAV_PARAM_TYPE_UINT32
    MAV_PARAM_TYPE_INT32 = mavutil.mavlink.MAV_PARAM_TYPE_INT32


    def __init__(self, mavlink_url: str = "udp:localhost:14550", source_system: int = 255, logger: Optional[logging.Logger] = None):
        """
        Initializes the MAVLink connection handler. Does not connect automatically.
        
        Args:
            mavlink_url (str): MAVLink connection string (e.g., 'udp:localhost:14550').
            source_system (int): MAVLink source system ID for this application.
            logger (logging.Logger): Logger instance.
        """
        _f_name = self.__class__.__name__+ '.' + self.__init__.__name__
        self.mavlink_url = mavlink_url if isinstance(mavlink_url, str) else "udp:localhost:14550"
        self.source_system = source_system if isinstance(source_system, int) and (0 <= source_system <= 255) else 255
        self.logger = logger if isinstance(logger, logging.Logger) else logging.getLogger(__name__)
        self.conn = None
        self.target_system = None
        self.target_component = None
        
        # Queues for received messages
        self.received_messages = queue.Queue(maxsize=500)
        self.received_messages_operation = queue.Queue(maxsize=100)
        
        # Threading control
        self.thread_receive_messages = None
        self.thread_receive_messages_stop_event = threading.Event()
        self.connection_failed_permanently = False
        
        # Operation Lock
        self._progress_operation: bool = False
        self._progress_operation_lock = threading.Lock()
        
        # --- Vehicle State Variables ---
        self._state_lock = threading.Lock()
        self._base_mode: Optional[int] = None
        self._custom_mode: Optional[int] = None
        self._last_global_position_int: Optional[Dict[str, Any]] = None
        self._last_battery_status: Optional[Dict[str, Any]] = None
        self._last_heartbeat_time: float = 0.0
        self._vehicle_type: Optional[int] = None
        self._autopilot_type: Optional[int] = None
        self._vehicle_system_status: Optional[int] = None # From HEARTBEAT.system_status
        self._last_attitude: Optional[Dict[str, Any]] = None
        self._last_local_position_ned: Optional[Dict[str, Any]] = None
        self._last_gps_raw_int: Optional[Dict[str, Any]] = None
        self._last_vfr_hud: Optional[Dict[str, Any]] = None
        self._last_extended_sys_state: Optional[Dict[str, Any]] = None
        self._last_vtol_state: Optional[int] = None # NEW: For VTOL state from EXTENDED_SYS_STATE
        # --- End Vehicle State Variables ---
        
        # Initialization
        try:
            self._create_receiver_thread()
            self.connect()
        
        except ConnectionError as e:
            self.logger.critical(f"{_f_name}: Initial MAVLink connection failed: {e}")
            raise
        
        except Exception as e:
            self.logger.critical(f"{_f_name}: Unexpected error during MAVLink init: {e}", exc_info=True)
            raise
        
        self.logger.info(f"{_f_name}: initialized with MAVLink URL: {self.mavlink_url}, source system: {self.source_system}")
    
    # --- Connection Management ---
    
    def _create_receiver_thread(self):
        """Creates the receiver thread object."""
        
        _f_name = self.__class__.__name__ + '.' + self._create_receiver_thread.__name__
        
        if self.thread_receive_messages and self.thread_receive_messages.is_alive():
            self.logger.warning(f"{_f_name}: Attempted to create receiver thread while it was already running.")
            return
        
        self.thread_receive_messages = threading.Thread(
            target=self._receive_message_thread,
            args=(self.thread_receive_messages_stop_event,),
            name="MAVLinkReceiverThread",
            daemon=True 
        )
        self.logger.debug(f"{_f_name}: MAVLink receiver thread object created.")
    
    def connect(self, item_timeout: float = 1.0, item_retries: int = 3):
        """
        Establishes MAVLink connection, waits for heartbeat, sets streamrates,
        and starts the receiver thread.
        
        Raises:
            ConnectionError: If connection fails after retries.
        """
        
        retries = 0
        max_retries = 5
        _f_name = self.__class__.__name__ + '.' + self.connect.__name__
        
        while not self.conn and retries < max_retries:
            try:
                self.logger.info(f"{_f_name}: Attempting MAVLink connection to {self.mavlink_url}...")
                
                if self.conn:
                    try: self.conn.close()
                    except Exception: pass
                    self.conn = None
                
                self.conn = mavutil.mavlink_connection(self.mavlink_url, source_system=self.source_system)
                self.logger.info(f"{_f_name}: Waiting for first heartbeat...")
                start_time = time.time()
                msg = None
                
                while time.time() - start_time < 10: # 10 second timeout
                    msg = self.conn.recv_match(type='HEARTBEAT', blocking=True, timeout=1)
                    if msg and msg.get_srcSystem() != self.source_system: 
                        break
                    else:
                        msg = None
                
                if msg:
                    self.target_system = msg.get_srcSystem()
                    self.target_component = msg.get_srcComponent() or 1 # Default component 1 if 0
                    self.logger.info(f"{_f_name}: Heartbeat received. Target system: {self.target_system}, component: {self.target_component}")
                    
                    with self._state_lock:
                        self._base_mode = msg.base_mode
                        self._custom_mode = msg.custom_mode
                        self._last_heartbeat_time = time.time()
                        
                        # Vehicle and autopilot types are also in HEARTBEAT
                        self._vehicle_type = msg.type
                        self._autopilot_type = msg.autopilot
                        self._vehicle_system_status = msg.system_status
                    
                    self.logger.info(f"{_f_name}: MAVLink connection established to {self.mavlink_url}")
                    self.logger.info(f"{_f_name}: Initial State: Armed={self._is_armed()}, Mode={self.get_flight_mode_str_local()}, VehicleType={self.get_vehicle_type_str()}, Autopilot={self.get_autopilot_type_str()}")
                    
                    if not self.thread_receive_messages or not self.thread_receive_messages.is_alive():
                        self.thread_receive_messages_stop_event.clear()
                        self.connection_failed_permanently = False
                        if not self.thread_receive_messages or self.thread_receive_messages.is_alive():
                            self._create_receiver_thread()
                        self.thread_receive_messages.start()
                        self.logger.info(f"{_f_name}: Receiver thread started successfully.")
                    else:
                        self.logger.warning(f"{_f_name}: Receiver thread was already running during connect().")
                    
                    self._set_initial_streamrates(item_timeout, item_retries)
                    return 
                else:
                    self.logger.warning(f"{_f_name}: No heartbeat received from target system within timeout.")
                    if self.conn: self.conn.close()
                    self.conn = None
            except Exception as e:
                self.logger.error(f"{_f_name}: MAVLink connection attempt failed: {str(e)}")
                if self.conn: self.conn.close()
                self.conn = None

            retries += 1
            if retries < max_retries:
                self.logger.warning(f"{_f_name}: Retrying connection to {self.mavlink_url} ({retries}/{max_retries})...")
                time.sleep(2)

        self.connection_failed_permanently = True
        raise ConnectionError(f"{_f_name}: Failed to establish MAVLink connection after {max_retries} attempts.")

    def reconnect(self, item_timeout: float = 1.0, item_retries: int = 3):
        """Attempts to close existing connection and establish a new one."""
        
        _f_name = self.__class__.__name__ + '.' + self.reconnect.__name__
        
        self.logger.info(f"{_f_name}: Attempting to reconnect to MAVLink...")
        
        if self.thread_receive_messages and self.thread_receive_messages.is_alive():
            self.thread_receive_messages_stop_event.set()
            self.thread_receive_messages.join(timeout=1.0)
            if self.thread_receive_messages.is_alive():
                self.logger.warning(f"{_f_name}: Receiver thread did not stop after 1 second.")
            
            self.thread_receive_messages = None

        self.target_system = None
        self.target_component = None
        if self.conn:
            try:
                self.conn.close()
            except Exception as close_err:
                self.logger.error(f"{_f_name}: Error closing existing MAVLink connection: {str(close_err)}")
        
        self.conn = None
        self.connection_failed_permanently = False 
        self._clear_queues(clear_general=True, clear_operation=True)
        
        try:
            self.connect(item_timeout, item_retries) 
            self.logger.info(f"{_f_name}: Reconnected to MAVLink successfully.")
            return True
        except ConnectionError as e:
            self.logger.critical(f"{_f_name}: Reconnection failed: {str(e)}")
            self.connection_failed_permanently = True 
            return False
        except Exception as e:
            self.logger.critical(f"{_f_name}: Unexpected error during reconnection: {str(e)}", exc_info=True)
            self.connection_failed_permanently = True
            return False


    def close(self):
        """Stops the receiver thread and closes the MAVLink connection."""
        
        _f_name = self.__class__.__name__ + '.' + self.close.__name__
        
        self.logger.info(f"{_f_name}: Closing MAVLink connection...")
        self.thread_receive_messages_stop_event.set()

        if self.thread_receive_messages and self.thread_receive_messages.is_alive():
            self.logger.info(f"{_f_name}: Waiting for receiver thread to stop...")
            self.thread_receive_messages.join(timeout=2.0)
            if self.thread_receive_messages.is_alive():
                self.logger.warning(f"{_f_name}: Receiver thread did not stop after 2 seconds.")
            self.thread_receive_messages = None 

        if self.conn:
            try:
                self.conn.close()
                self.logger.info(f"{_f_name}: MAVLink connection closed successfully.")
            except Exception as e:
                self.logger.error(f"{_f_name}: Error closing MAVLink connection: {str(e)}")

        self._clear_queues(clear_general=True, clear_operation=True)
        self.conn = None
        self.target_system = None
        self.target_component = None
        self.logger.info(f"{_f_name}: MAVLink connection closed and resources cleaned up.")

    # --- Message Handling & Helpers ---
    def is_ready(self) -> bool:
        return self._is_ready()
    
    def _is_ready(self) -> bool:
        """Checks if the connection is active and target is known."""
        
        _f_name = self.__class__.__name__ + '.' + self._is_ready.__name__
        
        if not self.conn:
            self.logger.error(f"{_f_name}: Operation failed: MAVLink connection not active.")
            return False
        
        if not self.target_system:
            self.logger.error(f"{_f_name}: Operation failed: Target system ID not available.")
            return False
        
        if not self.thread_receive_messages or not self.thread_receive_messages.is_alive():
            self.logger.error(f"{_f_name}: Operation failed: Receiver thread is not running.")
            return False
        
        return True
    
    def _clear_queues(self, clear_general: bool = False, clear_operation: bool = True):
        """Clears the specified message queues."""
        
        _f_name = self.__class__.__name__ + '.' + self._clear_queues.__name__
        
        clear_general = clear_general if isinstance(clear_general, bool) else False
        clear_operation = clear_operation if isinstance(clear_operation, bool) else True
        
        if clear_general:
            while not self.received_messages.empty():
                try: self.received_messages.get_nowait()
                except queue.Empty: break
            self.logger.debug(f"{_f_name}: General message queue cleared.")
        
        if clear_operation:
            while not self.received_messages_operation.empty():
                try: self.received_messages_operation.get_nowait()
                except queue.Empty: break
            self.logger.debug(f"{_f_name}: Operation message queue cleared.")

    def _replace_nan_with_none(self, obj: Any) -> Any:
        """Recursively replaces float NaN values with None."""
        if isinstance(obj, dict):
            return {k: self._replace_nan_with_none(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._replace_nan_with_none(elem) for elem in obj]
        elif isinstance(obj, float) and math.isnan(obj):
            return None
        else:
            return obj

    def _wait_for_message(self, expected_types: Union[str, List[str]] = None, timeout_per_attempt: float = 1.0, retries: int = 3, log_context: str = 'unknown') -> Optional[Dict[str, Any]]:
        """
        Waits for specific message type(s) from the operation queue.
        Handles potential None entries gracefully.
        """
        
        _f_name = self.__class__.__name__ + '.' + self._wait_for_message.__name__ + '.' + log_context
        
        if not isinstance(expected_types, (str, list)):
            self.logger.error(f"{_f_name}: Operation failed: Expected message types should be str or list, got {type(expected_types)}.")
            return None
        
        if not self._is_ready():
            self.logger.error(f"{_f_name}: Operation failed: MAVLink connection not active.")
            return None
        
        if not isinstance(timeout_per_attempt, (int, float)) or timeout_per_attempt <= 0: timeout_per_attempt = 1.0
        if not isinstance(retries, int) or retries < 0: retries = 0
        if not isinstance(log_context, str): log_context = 'unknown'
        
        if isinstance(expected_types, str):
            expected_types = [expected_types]
        
        total_attempts = retries + 1
        for attempt in range(total_attempts):
            try:
                msg_dict = self.received_messages_operation.get(block=True, timeout=timeout_per_attempt)
                
                if msg_dict is None:
                    self.logger.warning(f"{_f_name}: Received None message from operation queue. Skipping.")
                    continue 
                
                msg_mav_type = msg_dict.get('mavpackettype')
                self.logger.debug(f"{_f_name}: Received message from operation queue: {msg_dict}")
                
                if msg_mav_type in expected_types:
                    return msg_dict 
                else:
                    self.logger.debug(f"{_f_name}: Received message of type '{msg_mav_type}' not in expected types: {expected_types}.")
            except queue.Empty:
                self.logger.warning(f"{_f_name}: Operation message queue empty after {timeout_per_attempt} seconds. Retrying...")
            
            except Exception as e:
                self.logger.error(f"{_f_name}: Error while waiting for message: {str(e)}", exc_info=True)
                return None 
        
        self.logger.error(f"{_f_name}: Wait Loop ({log_context}): Max retries reached waiting for {expected_types}.")
        return None

    def _process_extended_sys_state(self, cleaned_dict: Dict[str, Any]) -> None:
        """Processes the EXTENDED_SYS_STATE message and updates the vehicle state."""
        
        _f_name = self.__class__.__name__ + '.' + self._process_extended_sys_state.__name__
        
        if not isinstance(cleaned_dict, dict):
            self.logger.error(f"{_f_name}: Invalid input: Expected dict, got {type(cleaned_dict)}.")
            return
        
        with self._state_lock: 
            
            self._last_extended_sys_state = cleaned_dict
            
            new_vtol_state_id = cleaned_dict.get('vtol_state')
            if self._last_vtol_state != new_vtol_state_id:
                old_vtol_state_for_log = self._last_vtol_state 
                self._last_vtol_state = new_vtol_state_id
                
                vtol_state_enum = mavutil.mavlink.enums.get('MAV_VTOL_STATE', {})
                vtol_state_str = "N/A"
                if new_vtol_state_id is not None:
                    state_obj = vtol_state_enum.get(new_vtol_state_id)
                    vtol_state_str = state_obj.name if state_obj and hasattr(state_obj, 'name') else f'UNKNOWN_VTOL_ID({new_vtol_state_id})'
                
                if old_vtol_state_for_log != new_vtol_state_id:
                    self.logger.debug(f"{_f_name}: VTOL State changed: {old_vtol_state_for_log} -> {new_vtol_state_id} ({vtol_state_str})")
        
            landed_state_val = cleaned_dict.get('landed_state')
            landed_state_str = "N/A"
            if landed_state_val is not None:
                landed_enum_obj = mavutil.mavlink.enums['MAV_LANDED_STATE'].get(landed_state_val)
                landed_state_str = landed_enum_obj.name if landed_enum_obj and hasattr(landed_enum_obj, 'name') else f'UNKNOWN_LANDED_ID({landed_state_val})'
            
            vtol_state_val = cleaned_dict.get('vtol_state')
            vtol_state_str = "N/A"
            if vtol_state_val is not None:
                vtol_enum_obj = mavutil.mavlink.enums['MAV_VTOL_STATE'].get(vtol_state_val)
                vtol_state_str = vtol_enum_obj.name if vtol_enum_obj and hasattr(vtol_enum_obj, 'name') else f'UNKNOWN_VTOL_ID({vtol_state_val})'
            self.logger.debug(f"{_f_name}: Extended Sys State: LandedState={landed_state_str}, VTOLState={vtol_state_str}")

    def _receive_message_thread(self, stop_event):
        """Target function for the receiver thread."""
        
        _f_name = self.__class__.__name__ + '.' + self._receive_message_thread.__name__
        
        self.logger.info(f"{_f_name}: Receiver thread started.")
        while not stop_event.is_set():
            if not self.conn:
                if self.connection_failed_permanently:
                    self.logger.critical(f"{_f_name}: Connection failed permanently. Stopping thread.")
                    break
                self.logger.warning(f"{_f_name}: Connection not established. Attempting to reconnect...")
                if not self.reconnect():
                    self.logger.critical(f"{_f_name}: Reconnect failed. Stopping thread.")
                    if self.connection_failed_permanently: 
                        break
                    time.sleep(5) 
                    continue
                else:
                    continue 

            try:
                msg = self.conn.recv_match(blocking=True, timeout=0.5)

                if msg:
                    if msg.get_srcSystem() == self.source_system:
                        continue
                    msg_type_str = msg.get_type() 
                    if msg_type_str.startswith('UNKNOWN_'): 
                        self.logger.warning(f"{_f_name}: Received unknown message type: {msg_type_str}. Skipping.")
                        continue

                    try:
                        msg_dict = msg.to_dict()
                        cleaned_dict = self._replace_nan_with_none(msg_dict)
                        mav_packet_type = cleaned_dict.get('mavpackettype') 

                        # --- Process State Updates ---
                        if mav_packet_type == 'HEARTBEAT':
                            with self._state_lock:
                                self._base_mode = cleaned_dict.get('base_mode')
                                self._custom_mode = cleaned_dict.get('custom_mode')
                                self._last_heartbeat_time = time.time()
                                new_vehicle_type = cleaned_dict.get('type')
                                new_autopilot_type = cleaned_dict.get('autopilot')
                                new_system_status = cleaned_dict.get('system_status')
                                if self._vehicle_type != new_vehicle_type: self._vehicle_type = new_vehicle_type
                                if self._autopilot_type != new_autopilot_type: self._autopilot_type = new_autopilot_type
                                if self._vehicle_system_status != new_system_status: self._vehicle_system_status = new_system_status
                            self.logger.debug(f"{_f_name}: Heartbeat Update: BaseMode={cleaned_dict.get('base_mode')}, CustomMode={cleaned_dict.get('custom_mode')}, SystemStatus={cleaned_dict.get('system_status')}")

                        elif mav_packet_type == 'GLOBAL_POSITION_INT':
                            with self._state_lock:
                                self._last_global_position_int = cleaned_dict
                            self.logger.debug(f"{_f_name}: GlobalPositionInt Update: Lat={cleaned_dict.get('lat', 0)}, Lon={cleaned_dict.get('lon', 0)}, Alt={cleaned_dict.get('alt', 0)}")
                        elif mav_packet_type == 'BATTERY_STATUS':
                            with self._state_lock:
                                self._last_battery_status = cleaned_dict
                            self.logger.debug(f"{_f_name}: Battery Status Update: Voltage={cleaned_dict.get('voltage_cell', 0.0):.2f}, Current={cleaned_dict.get('current_battery', 0.0):.2f}, Remaining={cleaned_dict.get('battery_remaining', 0)}")
                        elif mav_packet_type == 'ATTITUDE':
                            with self._state_lock:
                                self._last_attitude = cleaned_dict
                            self.logger.debug(f"{_f_name}: Attitude Update: Roll={cleaned_dict.get('roll', 0.0):.2f}, Pitch={cleaned_dict.get('pitch', 0.0):.2f}, Yaw={cleaned_dict.get('yaw', 0.0):.2f}")
                        elif mav_packet_type == 'LOCAL_POSITION_NED':
                            with self._state_lock:
                                self._last_local_position_ned = cleaned_dict
                            self.logger.debug(f"{_f_name}: LocalPositionNED Update: X={cleaned_dict.get('x', 0.0):.2f}, Y={cleaned_dict.get('y', 0.0):.2f}, Z={cleaned_dict.get('z', 0.0):.2f}")
                        elif mav_packet_type == 'GPS_RAW_INT':
                            with self._state_lock:
                                self._last_gps_raw_int = cleaned_dict
                            self.logger.debug(f"{_f_name}: GPSRawInt Update: Fix={cleaned_dict.get('fix_type')}, Sats={cleaned_dict.get('satellites_visible')}")
                        elif mav_packet_type == 'VFR_HUD':
                            with self._state_lock:
                                self._last_vfr_hud = cleaned_dict
                            self.logger.debug(f"{_f_name}: VFR HUD Update: Airspeed={cleaned_dict.get('airspeed', 0.0):.2f}, Groundspeed={cleaned_dict.get('groundspeed', 0.0):.2f}, Altitude={cleaned_dict.get('alt', 0.0):.2f}")
                        
                        elif mav_packet_type == 'EXTENDED_SYS_STATE':
                            self._process_extended_sys_state(cleaned_dict)

                        elif mav_packet_type == 'STATUSTEXT':
                            self.logger.info(f"{_f_name}: Status Text: {cleaned_dict.get('text')}")
                        
                        # --- Routing Logic ---
                        if mav_packet_type in ['MISSION_COUNT', 'MISSION_REQUEST', 'MISSION_REQUEST_INT',
                                               'MISSION_ITEM', 'MISSION_ITEM_INT', 'MISSION_ACK',
                                               'COMMAND_ACK', 'PARAM_VALUE']:
                            try:
                                self.received_messages_operation.put_nowait(cleaned_dict)
                            except queue.Full:
                                self.logger.warning(f"{_f_name}: Operation message queue full. Discarding cleaned {mav_packet_type} message.")
                                dropped_item = self.received_messages_operation.get_nowait()
                                self.logger.warning(f"{_f_name}: Dropped item from operation queue: {dropped_item}")
                                self.received_messages_operation.put_nowait(cleaned_dict)
                        else:
                            try:
                                self.received_messages.put_nowait(cleaned_dict)
                            except queue.Full:
                                self.logger.warning(f"{_f_name}: General message queue full. Discarding cleaned {mav_packet_type} message.")
                                dropped_item = self.received_messages.get_nowait()
                                self.logger.warning(f"{_f_name}: Dropped item from general queue: {dropped_item}")
                                self.received_messages.put_nowait(cleaned_dict)
                    except AttributeError: 
                        self.logger.error(f"{_f_name}: Error converting message to dict: {msg}. Message type: {msg_type_str}", exc_info=True)
                    except Exception as proc_err:
                        self.logger.error(f"{_f_name}: Error processing message: {msg}. Message type: {msg_type_str}. Error: {str(proc_err)}", exc_info=True)
                
                if stop_event.is_set():
                    break
            except (OSError, mavutil.mavlink.MAVError, BrokenPipeError, ConnectionResetError, TimeoutError) as e:
                self.logger.error(f"{_f_name}: MAVLink receive error: {str(e)}", exc_info=True)
                if not self.reconnect():
                    self.logger.critical(f"{_f_name}: Reconnect failed after error: {str(e)}")
                    self.connection_failed_permanently = True 
                    break 
            except Exception as e:
                self.logger.error(f"{_f_name}: Unexpected error in receiver thread: {str(e)}", exc_info=True)
                time.sleep(1) 
        
        self.logger.info(f"{_f_name}: Receiver thread stopped.")
    
    # --- Command Handling ---
    def _single_command(self, command_data: Dict[str, Any] = None, item_timeout = 1.0, retries = 3) -> bool:
        """Processes, sends, and waits for ACK for a single command."""
        
        _f_name = self.__class__.__name__ + '.' + self._single_command.__name__
        
        if not isinstance(command_data, dict):
            self.logger.error(f"{_f_name}: Invalid command format: Expected dict, got {type(command_data)}. Command: {command_data}")
            return False
        
        command_type = command_data.get('type')
        command_id = command_data.get('command')
        
        if not command_type or command_id is None:
            self.logger.error(f"{_f_name}: Invalid command content: Missing 'type' or 'command' key. Command: {command_data}")
            return False
        
        if not self._is_ready(): return False
        
        if not self._start_operation(): return False
        
        try:
            command_id = int(command_id) 
            
            self._clear_queues(clear_operation=True)
            
            if command_type == 'COMMAND_LONG':
                defaults = {"confirmation": 0,"param1": 0.0, "param2": 0.0, "param3": 0.0,
                            "param4": 0.0, "param5": 0.0, "param6": 0.0, "param7": 0.0}
                
                params = {key: float(command_data.get(key, defaults[key])) for key in defaults if key != "confirmation"}
                confirmation = int(command_data.get("confirmation", defaults["confirmation"]))
                
                self.logger.info(f"{_f_name}: Sending COMMAND_LONG (ID: {command_id}) with params: {params}")
                self.conn.mav.command_long_send(self.target_system, self.target_component, command_id, confirmation,
                                                params["param1"], params["param2"], params["param3"], params["param4"],
                                                params["param5"], params["param6"], params["param7"])
            
            elif command_type == 'COMMAND_INT':
                defaults = {"frame": int(mavutil.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT_INT), "current": 0, "autocontinue": 1,
                            "param1": 0.0, "param2": 0.0, "param3": 0.0, "param4": 0.0,
                            "param5": 0, "param6": 0, "param7": 0.0} 
                frame = int(command_data.get("frame", defaults["frame"]))
                current = int(command_data.get("current", defaults["current"]))
                autocontinue = int(command_data.get("autocontinue", defaults["autocontinue"]))
                param1 = float(command_data.get("param1", defaults["param1"]))
                param2 = float(command_data.get("param2", defaults["param2"]))
                param3 = float(command_data.get("param3", defaults["param3"]))
                param4 = float(command_data.get("param4", defaults["param4"]))
                param5 = int(command_data.get("param5", command_data.get("x", defaults["param5"]))) 
                param6 = int(command_data.get("param6", command_data.get("y", defaults["param6"]))) 
                param7 = float(command_data.get("param7", command_data.get("z", defaults["param7"]))) 
                
                self.logger.info(f"{_f_name}: Sending COMMAND_INT (ID: {command_id}, Frame: {frame}) with p1-4: ({param1}, {param2}, {param3}, {param4}), p5-7: ({param5}, {param6}, {param7})")
                self.conn.mav.command_int_send(self.target_system, self.target_component, frame, command_id, current,
                                            autocontinue, param1, param2, param3, param4, param5, param6, param7)
            
            else:
                self.logger.error(f"{_f_name}: Unsupported command 'type': {command_type}. Use 'COMMAND_LONG' or 'COMMAND_INT'.")
                return False
            
            ack_msg = self._wait_for_message('COMMAND_ACK', item_timeout, retries, f"{_f_name}: {command_type}: {command_id}")
            
            if ack_msg and ack_msg.get('command') == command_id:
                ack_result = ack_msg.get('result')
                ack_result_enum = mavutil.mavlink.enums['MAV_RESULT'].get(ack_result, f'{_f_name}: UNKNOWN({ack_result})')
                ack_result_name = ack_result_enum.name if hasattr(ack_result_enum, 'name') else str(ack_result_enum)
                
                if ack_result == self.CMD_ACCEPTED:
                    self.logger.info(f"{_f_name}: Command {command_type} (ID: {command_id}) acknowledged successfully (Result: {ack_result_name}).")
                    return True
                
                else:
                    self.logger.error(f"{_f_name}: Command {command_type} (ID: {command_id}) failed or rejected with result: {ack_result_name}.")
                    progress = ack_msg.get('progress')
                    
                    if progress is not None:
                        self.logger.warning(f"{_f_name}: Command {command_id} ACK progress field: {progress}")
                    
                    return False
            
            elif ack_msg is None:
                self.logger.error(f"{_f_name}: Timeout waiting for ACK for command {command_type} (ID: {command_id}).")
                return False
            
            else:
                self.logger.error(f"{_f_name}: Received incorrect or invalid ACK for command {command_type} (ID: {command_id}). Message: {ack_msg}")
                return False
        
        except (ValueError, TypeError) as e:
            self.logger.error(f"{_f_name}: Error processing command parameters for '{command_type}' (ID: {command_id}): {str(e)} - Data: {command_data}", exc_info=True)
            return False
        
        except Exception as e:
            self.logger.error(f"{_f_name}: Error sending/waiting for ACK for command '{command_type}' (ID: {command_id}): {str(e)}", exc_info=True)
            return False
        
        finally:
            self._end_operation()
            self._clear_queues(clear_operation=True)
    
    def send_command(self, data: Union[Dict[str, Any], List[Dict[str, Any]]] = None, item_timeout = 1.0, retries = 3) -> bool:
        
        _f_name = self.__class__.__name__ + '.' + self.send_command.__name__
        
        if isinstance(data, list):
            if len(data) == 0:
                self.logger.info(f"{_f_name}: Empty command list provided. Nothing to send.")
                return True
            
            self.logger.info(f"{_f_name}: Processing list of {len(data)} commands...")
            
            for idx, command_dict in enumerate(data):
                self.logger.debug(f"{_f_name}: Sending command {idx + 1}/{len(data)}...")
                
                if not self._single_command(command_dict, item_timeout, retries):
                    self.logger.error(f"{_f_name}: Failed processing command at index {idx} in list. Aborting sequence.")
                    return False
            
            self.logger.info(f"{_f_name}: {len(data)} All commands in list processed successfully.")
            return True
        
        elif isinstance(data, dict):
            self.logger.info(f"{_f_name}: Processing single command...")
            if not self._single_command(data, item_timeout, retries):
                self.logger.error(f"{_f_name}: Failed processing single command. Aborting.")
                return False
            
            self.logger.info(f"{_f_name}: Single command processed successfully.")
            return True
        
        else:
            self.logger.error(f"{_f_name}: Invalid command data format: Expected dict or list, got {type(data)}.")
            return False
    
    def _start_operation(self) -> bool:
        """Attempts to acquire the operation progress lock."""
        
        _f_name = self.__class__.__name__ + '.' + self._start_operation.__name__
        
        if not self._progress_operation_lock.acquire(blocking=False):
            self.logger.warning(f"{_f_name}: Another operation (command/mission) is already in progress. Aborting new request.")
            return False
        
        self._progress_operation = True
        self.logger.debug(f"{_f_name}: Acquired operation progress lock.")
        return True
    
    def _end_operation(self):
        """Releases the operation progress lock."""
        
        _f_name = self.__class__.__name__ + '.' + self._end_operation.__name__
        
        if self._progress_operation: 
            self._progress_operation = False
            try:
                self._progress_operation_lock.release()
                self.logger.debug(f"{_f_name}: Released operation progress lock.")
            except (threading.ThreadError, RuntimeError):
                self.logger.warning(f"{_f_name}: Attempted to release operation lock that was not held or already unlocked.", exc_info=True)
        else:
            self.logger.warning(f"{_f_name}: Attempted to end operation when flag indicates no operation was in progress.")
    
    def mission_get(self, item_timeout = 1.0, retries = 3) -> Optional[List[Dict[str, Any]]]:
        """Downloads the mission from the vehicle."""
        
        _f_name = self.__class__.__name__ + '.' + self.mission_get.__name__
        
        if not self._is_ready(): return None
        if not self._start_operation(): return None
        
        self.logger.info(f"{_f_name}: Starting mission download...")
        self._clear_queues(clear_operation=True)
        expected_count = None
        mission_items = {}
        success = False
        result_list = None

        try:
            self.logger.info(f"{_f_name}: Sending MISSION_REQUEST_LIST to vehicle...")
            self.conn.mav.mission_request_list_send(self.target_system, self.target_component, self.MISSION_TYPE)
            count_msg = self._wait_for_message('MISSION_COUNT', item_timeout, retries, _f_name )

            if count_msg and count_msg.get('mission_type') == self.MISSION_TYPE:
                expected_count = count_msg.get('count')
                if expected_count is None:
                    self.logger.error(f"{_f_name}: MISSION_COUNT message received but count is None. Aborting.")
                    return None 
                self.logger.info(f"{_f_name}: MISSION_COUNT received: {expected_count} items.")
            else:
                self.logger.error(f"{_f_name}: Failed to receive MISSION_COUNT message. Aborting.")
                return None

            if expected_count == 0:
                self.logger.info(f"{_f_name}: No mission items to download. Sending MISSION_ACK (ACCEPTED).")
                self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ACCEPTED, self.MISSION_TYPE)
                success = True
                result_list = []
                return result_list 

            seq_expected = 0
            while seq_expected < expected_count:
                self.logger.debug(f"{_f_name}: Requesting MISSION_ITEM_INT for seq {seq_expected}...")
                self.conn.mav.mission_request_int_send(self.target_system, self.target_component, seq_expected, self.MISSION_TYPE)
                item_msg = self._wait_for_message('MISSION_ITEM_INT', item_timeout, retries, _f_name)

                if item_msg and item_msg.get('mission_type') == self.MISSION_TYPE:
                    msg_seq = item_msg.get('seq')
                    if msg_seq == seq_expected:
                        self.logger.debug(f"{_f_name}: Received MISSION_ITEM_INT for seq {msg_seq}")
                        mission_items[msg_seq] = item_msg
                        seq_expected += 1
                    else:
                        self.logger.error(f"{_f_name}: Received MISSION_ITEM_INT with unexpected seq {msg_seq}. Expected {seq_expected}.")
                        return None 
                else:
                    self.logger.error(f"{_f_name}: Failed to receive MISSION_ITEM_INT message. Aborting.")
                    return None

            if len(mission_items) == expected_count:
                self.logger.info(f"{_f_name}: All {expected_count} mission items downloaded successfully.")
                self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ACCEPTED, self.MISSION_TYPE)
                ordered_items = [mission_items[i] for i in sorted(mission_items.keys())]
                success = True
                result_list = ordered_items
                return result_list
            else:
                self.logger.error(f"{_f_name}: Mismatch in expected and received mission items. Expected {expected_count}, received {len(mission_items)}.")
                self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ERROR, self.MISSION_TYPE)
                return None

        except Exception as e:
            self.logger.error(f"{_f_name}: Error during mission download: {str(e)}", exc_info=True)
            if self.conn and self.target_system:
                try:
                    self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ERROR, self.MISSION_TYPE)
                except Exception:
                    pass
            return None 
        
        finally:
            self._end_operation() 
            if not success:
                self.logger.error(f"{_f_name}: Mission download failed. Cleaning up.")

    def mission_set(self, items: List[Dict[str, Any]], item_timeout = 1.0, retries = 3) -> bool:
        """Uploads a mission to the vehicle using MISSION_ITEM_INT."""
        
        _f_name = self.__class__.__name__ + '.' + self.mission_set.__name__
        
        if not isinstance(items, list):
            self.logger.error(f"{_f_name}: Invalid items format: Expected list, got {type(items)}. Items: {items}")
            return False
        
        if not self._is_ready(): return False
        if not self._start_operation(): return False

        expected_count = len(items)
        self.logger.info(f"{_f_name}: Starting mission upload with {expected_count} items.")
        self._clear_queues(clear_operation=True)
        success = False

        try:
            self.logger.info(f"{_f_name}: Sending MISSION_COUNT with {expected_count} items...")
            self.conn.mav.mission_count_send(self.target_system, self.target_component, expected_count, self.MISSION_TYPE)

            if expected_count == 0:
                self.logger.info(f"{_f_name}: No mission items to upload. Sending MISSION_ACK (ACCEPTED).")
                ack_or_req = self._wait_for_message(['MISSION_ACK', 'MISSION_REQUEST_INT', 'MISSION_REQUEST'], item_timeout, retries, _f_name)

                if ack_or_req and ack_or_req.get('mavpackettype') == 'MISSION_ACK':
                    ack_type = ack_or_req.get('type')
                    ack_type_name = mavutil.mavlink.enums['MAV_MISSION_RESULT'].get(ack_type, f'UNKNOWN_ACK_TYPE({ack_type})').name
                    if ack_type == self.MISSION_ACCEPTED:
                        self.logger.info(f"{_f_name}: Empty mission upload acknowledged successfully (ACK type: {ack_type_name}).")
                        success = True
                        return True
                    else:
                        self.logger.error(f"{_f_name}: Empty mission upload failed with ACK type: {ack_type_name}.")
                        return False
                elif ack_or_req and ack_or_req.get('mavpackettype') in ['MISSION_REQUEST_INT', 'MISSION_REQUEST']:
                    self.logger.info(f"{_f_name}: Received MISSION_REQUEST_INT/REQ after sending MISSION_COUNT=0. Sending MISSION_ACK (ACCEPTED).")
                    self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ACCEPTED, self.MISSION_TYPE)
                    success = True 
                    return True
                else:
                    self.logger.error(f"{_f_name}: Failed to receive valid MISSION_ACK or MISSION_REQUEST after sending MISSION_COUNT=0. Aborting.")
                    self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ERROR, self.MISSION_TYPE)
                    return False

            current_seq = 0
            while current_seq < expected_count:
                req_msg = self._wait_for_message(['MISSION_REQUEST_INT', 'MISSION_REQUEST'], item_timeout, retries, _f_name)

                if req_msg:
                    req_mission_type = req_msg.get('mission_type')
                    if req_mission_type is not None and req_mission_type != self.MISSION_TYPE:
                        self.logger.error(f"{_f_name}: Received MISSION_REQUEST with unexpected mission type {req_mission_type}. Expected {self.MISSION_TYPE}.")
                        self.conn.mav.mission_ack_send(self.target_system, self.target_component, mavutil.mavlink.MAV_MISSION_INVALID_PARAM7, self.MISSION_TYPE) 
                        return False

                    req_seq = req_msg.get('seq')
                    if req_seq == current_seq:
                        item = items[current_seq]
                        self.logger.debug(f"{_f_name}: Sending MISSION_ITEM_INT for seq {current_seq}...")
                        try:
                            frame = int(item.get('frame', mavutil.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT_INT))
                            command = int(item.get('command', 0))
                            cur = int(item.get('current', 0)) 
                            autocontinue = int(item.get('autocontinue', 1)) 
                            param1 = float(item.get('param1', 0.0))
                            param2 = float(item.get('param2', 0.0))
                            param3 = float(item.get('param3', 0.0))
                            param4 = float(item.get('param4', 0.0))
                            param5 = int(item.get('param5', item.get('x', 0))) 
                            param6 = int(item.get('param6', item.get('y', 0))) 
                            param7 = float(item.get('param7', item.get('z', 0.0))) 

                            self.conn.mav.mission_item_int_send(
                                self.target_system, self.target_component, current_seq, frame, command, cur, autocontinue,
                                param1, param2, param3, param4, param5, param6, param7, self.MISSION_TYPE
                            )
                            current_seq += 1 
                        except (ValueError, TypeError, KeyError) as send_err:
                            self.logger.error(f"{_f_name}: Error sending MISSION_ITEM_INT for seq {current_seq}: {str(send_err)}", exc_info=True)
                            self.conn.mav.mission_ack_send(self.target_system, self.target_component, mavutil.mavlink.MAV_MISSION_INVALID_PARAM1, self.MISSION_TYPE) 
                            return False
                        except Exception as send_err:
                            self.logger.error(f"{_f_name}: Unexpected error sending MISSION_ITEM_INT for seq {current_seq}: {str(send_err)}", exc_info=True)
                            self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ERROR, self.MISSION_TYPE)
                            return False
                    else:
                        self.logger.error(f"{_f_name}: Received MISSION_REQUEST with unexpected seq {req_seq}. Expected {current_seq}.")
                        if req_msg.get('mavpackettype') == 'MISSION_ACK':
                            ack_type = req_msg.get('type', self.MISSION_ERROR)
                            ack_type_name = mavutil.mavlink.enums['MAV_MISSION_RESULT'].get(ack_type, f'UNKNOWN_ACK_TYPE({ack_type})').name
                            self.logger.error(f"{_f_name}: Received MISSION_ACK with unexpected seq {req_seq}. Expected {current_seq}. ACK type: {ack_type_name}.")
                            return False 
                        else:
                            self.conn.mav.mission_ack_send(self.target_system, self.target_component, mavutil.mavlink.MAV_MISSION_INVALID_SEQUENCE, self.MISSION_TYPE)
                            return False
                else:
                    self.logger.error(f"{_f_name}: Timeout waiting for MISSION_REQUEST_INT/REQ. Aborting.")
                    return False

            self.logger.info(f"{_f_name}: All {expected_count} mission items uploaded successfully. Sending MISSION_ACK (ACCEPTED).")
            final_ack_msg = self._wait_for_message('MISSION_ACK', item_timeout, retries, _f_name)

            if final_ack_msg and final_ack_msg.get('mission_type') == self.MISSION_TYPE and final_ack_msg.get('type') == self.MISSION_ACCEPTED:
                self.logger.info(f"{_f_name}: MISSION_ACK received: {final_ack_msg.get('type')}")
                success = True
                return True
            else:
                ack_type = final_ack_msg.get('type') if final_ack_msg else 'Timeout'
                ack_enum = mavutil.mavlink.enums['MAV_MISSION_RESULT'].get(ack_type, f'UNKNOWN({ack_type})')
                ack_type_name = ack_enum.name if hasattr(ack_enum, 'name') else str(ack_enum)
                self.logger.error(f"{_f_name}: MISSION_ACK not received or invalid. Received: {ack_type_name}.")
                if final_ack_msg and final_ack_msg.get('mavpackettype') == 'MISSION_REQUEST_INT':
                    self.logger.error(f"{_f_name}: Received MISSION_REQUEST_INT after sending MISSION_ACK. This is unexpected.")
                    self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ERROR, self.MISSION_TYPE)
                return False

        except Exception as e:
            self.logger.error(f"{_f_name}: Error during mission upload: {str(e)}", exc_info=True)
            if self.conn and self.target_system:
                try:
                    self.conn.mav.mission_ack_send(self.target_system, self.target_component, self.MISSION_ERROR, self.MISSION_TYPE)
                except Exception:
                    pass
            return False
        finally:
            self._end_operation() 
            if not success:
                self.logger.error(f"{_f_name}: Mission upload failed. Cleaning up.")

    # --- Vehicle State Getters ---

    def _is_armed(self) -> bool:
        """Returns the last known armed status based on HEARTBEAT."""
        
        _f_name = self.__class__.__name__ + '.' + self._is_armed.__name__
        
        with self._state_lock:
            if self._base_mode is None:
                self.logger.warning(f"{_f_name}: Vehicle armed status unknown: No HEARTBEAT received yet.")
                return False 
            return bool(self._base_mode & self.MAV_MODE_FLAG_SAFETY_ARMED)

    def get_flight_mode_str_local(self) -> Optional[str]:
        """
        Internal: Returns the flight mode name as a string based on HEARTBEAT.
        Provides a generic representation (e.g., "CUSTOM(4)").
        Child classes should override get_flight_mode() for specific interpretations.
        """
        
        _f_name = self.__class__.__name__ + '.' + self.get_flight_mode_str_local.__name__
        
        with self._state_lock:
            if self._base_mode is None or self._custom_mode is None:
                 self.logger.warning(f"{_f_name}: Vehicle flight mode unknown: No HEARTBEAT received yet.")
                 return None

            if bool(self._base_mode & self.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED):
                return f"CUSTOM({self._custom_mode})"
            else:
                # Attempt to get the name from MAV_MODE enum
                base_mode_enum_val = mavutil.mavlink.enums['MAV_MODE'].get(self._base_mode)
                if base_mode_enum_val:
                    return base_mode_enum_val.name
                else:
                    return f"BASE({self._base_mode})"

    def get_flight_mode(self) -> Optional[str]:
        """
        Returns the flight mode name as a string.
        This base implementation provides a generic interpretation from HEARTBEAT.
        It is expected to be overridden by vehicle-specific handlers.
        """
        return self.get_flight_mode_str_local()

    def get_position(self) -> Optional[Dict[str, Any]]:
        
        _f_name = self.__class__.__name__ + '.' + self.get_position.__name__
        
        with self._state_lock: 
            if self._last_global_position_int is None:
                 self.logger.warning(f"{_f_name}: Vehicle position unknown: No GLOBAL_POSITION_INT message received yet.")
                 return None
            try:
                pos_data = {
                    'lat': self._last_global_position_int.get('lat', 0) / 1e7,
                    'lon': self._last_global_position_int.get('lon', 0) / 1e7,
                    'alt_rel_m': self._last_global_position_int.get('relative_alt', 0) / 1000.0,
                    'alt_msl_m': self._last_global_position_int.get('alt', 0) / 1000.0,
                    'heading_deg': (hdg / 100.0) if (hdg := self._last_global_position_int.get('hdg')) is not None and hdg != 65535 else None,
                    'time_boot_ms': self._last_global_position_int.get('time_boot_ms')
                }
                return pos_data
            except Exception as e:
                self.logger.error(f"{_f_name}: Error processing position data: {str(e)}", exc_info=True)
                return None

    def get_vehicle_type_id(self) -> Optional[int]:
        """Returns the detected MAV_TYPE ID from the vehicle's HEARTBEAT."""
        with self._state_lock:
            return self._vehicle_type

    def get_vehicle_type_str(self) -> Optional[str]:
        """Returns the string representation of the detected MAV_TYPE."""
        with self._state_lock:
            if self._vehicle_type is None:
                return None
            type_enum = mavutil.mavlink.enums['MAV_TYPE'].get(self._vehicle_type)
            return type_enum.name if type_enum and hasattr(type_enum, 'name') else f'UNKNOWN_MAV_TYPE({self._vehicle_type})'

    def get_autopilot_type_id(self) -> Optional[int]:
        """Returns the detected MAV_AUTOPILOT ID from the vehicle's HEARTBEAT."""
        with self._state_lock:
            return self._autopilot_type

    def get_autopilot_type_str(self) -> Optional[str]:
        """Returns the string representation of the detected MAV_AUTOPILOT type."""
        with self._state_lock:
            if self._autopilot_type is None:
                return None
            autopilot_enum = mavutil.mavlink.enums['MAV_AUTOPILOT'].get(self._autopilot_type)
            return autopilot_enum.name if autopilot_enum and hasattr(autopilot_enum, 'name') else f'UNKNOWN_AUTOPILOT({self._autopilot_type})'

    def get_vehicle_system_status_id(self) -> Optional[int]:
        """Returns the MAV_STATE ID from the vehicle's HEARTBEAT (system_status field)."""
        with self._state_lock:
            return self._vehicle_system_status

    def get_vehicle_system_status_str(self) -> Optional[str]:
        """Returns the string representation of the MAV_STATE (system_status field)."""
        with self._state_lock:
            if self._vehicle_system_status is None:
                return None
            status_enum = mavutil.mavlink.enums['MAV_STATE'].get(self._vehicle_system_status)
            return status_enum.name if status_enum and hasattr(status_enum, 'name') else f'UNKNOWN_MAV_STATE({self._vehicle_system_status})'


    # --- Stream Rate Setting ---
    def set_streamrates(self, desired_rates_hz: Dict[int, int] = None, item_timeout: float = 1.0, retries: int = 3) -> bool:
        """
        Internal: Takes a dictionary of MessageID:Frequency(Hz), converts it
        into a list of MAV_CMD_SET_MESSAGE_INTERVAL commands, and sends them.
        """
        
        _f_name = self.__class__.__name__ + '.' + self.set_streamrates.__name__
        
        if not isinstance(desired_rates_hz, dict):
            self.logger.error(f"{_f_name}: Invalid input. Expected Dict[int, int], got {type(desired_rates_hz)}")
            return False
        
        if len(desired_rates_hz) == 0:
            self.logger.info(f"{_f_name}: Empty dictionary provided. Nothing to do.")
            return True
        
        self.logger.info(f"{_f_name}: Processing {len(desired_rates_hz)} stream rate requests...")
        commands_to_send = []
        
        for msg_id, freq_hz in desired_rates_hz.items():
            if not isinstance(msg_id, int):
                self.logger.warning(f"{_f_name}: Skipping invalid message ID (not int): {msg_id}")
                continue
            
            if not isinstance(freq_hz, int):
                self.logger.warning(f"{_f_name}: Skipping invalid frequency for MsgID {msg_id} (not int): {freq_hz}")
                continue
            
            if freq_hz == -1: interval_us = -1
            elif freq_hz == 0: interval_us = 0
            elif freq_hz > 0: interval_us = int(1_000_000 / freq_hz)
            
            else:
                self.logger.warning(f"{_f_name}: Invalid frequency {freq_hz}Hz for MsgID {msg_id}. Use >=0 or -1.")
                continue
            
            self.logger.debug(f"{_f_name}: Preparing command for MsgID {msg_id} at {freq_hz} Hz (Interval: {interval_us} us)")
            
            command_dict = {
                "type": "COMMAND_LONG",
                "command": self.CMD_SET_MESSAGE_INTERVAL,
                "param1": float(msg_id),      
                "param2": float(interval_us), 
            }
            commands_to_send.append(command_dict)
        
        if not commands_to_send:
            self.logger.warning(f"{_f_name}: No valid stream rate commands generated from the input dictionary.")
            return True 
        
        self.logger.info(f"{_f_name}: Sending {len(commands_to_send)} generated stream rate commands via self.command...")
        
        if self.send_command(commands_to_send, item_timeout, retries):
            self.logger.info(f"{_f_name}: All stream rate commands acknowledged successfully.")
            return True
        
        else:
            self.logger.error(f"{_f_name}: Failed to set some or all stream rates based on ACK results.")
            return False

    def _set_initial_streamrates(self, item_timeout: float = 1.0, retries: int = 3) -> bool:
        """
        Defines the initial desired stream rates and calls _set_streamrates.
        Vehicle-specific handlers can override this or call set_message_stream_rates_config
        in their __init__ if different initial rates are needed.
        """
        
        _f_name = self.__class__.__name__ + '.' + self._set_initial_streamrates.__name__
        
        self.logger.info(f"{_f_name}: Defining initial message streamrates...")
        
        initial_desired_rates: Dict[int, int] = {
            self.HEARTBEAT_TYPE: 1,
            self.SYS_STATUS_TYPE_ID: 1, 
            self.EXTENDED_SYS_STATE_TYPE_ID: 2,
            self.BATTERY_STATUS_TYPE: 1,
            self.GLOBAL_POSITION_INT_TYPE: 5,
            self.ATTITUDE_TYPE_ID: 10,
            self.LOCAL_POSITION_NED_TYPE_ID: 5,
            self.GPS_RAW_INT_TYPE_ID: 1,
            self.VFR_HUD_TYPE_ID: 2,
        }
        if self.set_streamrates(initial_desired_rates, item_timeout, retries):
            self.logger.info(f"{_f_name}: Initial stream rates set successfully.")
            return True
        
        else:
            self.logger.error(f"{_f_name}: Failed to set some or all initial stream rates based on ACK results.")
            return False
    
    # --- Internal Parameter Handling Methods ---
    def get_param(self, param_id_str: Optional[str] = None, item_timeout: float = 1.0, retries: int = 3) -> Optional[float]:
        """
        Internal: Sends PARAM_REQUEST_READ and waits for PARAM_VALUE.
        Assumes operation lock is already held.
        """
        
        _f_name = self.__class__.__name__ + '.' + self.get_param.__name__
        
        if not isinstance(param_id_str, str):
            self.logger.error(f"{_f_name}: Invalid parameter ID type: {type(param_id_str)}. Expected str.")
            return None
        
        if not self._is_ready(): return None
        if not self._start_operation(): return None        
        
        self._clear_queues(clear_operation=True)
        self.logger.debug(f"{_f_name}: Requesting parameter '{param_id_str}'...")
        param_value_received = None
        
        try:
            self.conn.mav.param_request_read_send(
                self.target_system,
                self.target_component,
                param_id_str, 
                -1  
            )

            response_msg = self._wait_for_message(
                'PARAM_VALUE', 
                item_timeout,
                retries,
                _f_name
            )

            if response_msg:
                received_param_id = response_msg.get('param_id')
                if received_param_id == param_id_str:
                    param_value_received = response_msg.get('param_value')
                    param_type = response_msg.get('param_type') 
                    self.logger.debug(f"{_f_name}: Received PARAM_VALUE for '{param_id_str}': {param_value_received} (Type: {param_type})")
                    if param_value_received is None:
                        self.logger.error(f"{_f_name}: PARAM_VALUE for '{param_id_str}' received, but 'param_value' field is None.")
                else:
                    self.logger.warning(f"{_f_name}: Received PARAM_VALUE for different ID: '{received_param_id}' (expected '{param_id_str}').")
            else:
                self.logger.error(f"{_f_name}: Timeout waiting for PARAM_VALUE for '{param_id_str}'.")
                param_value_received = None
        
        except (ValueError, TypeError) as e:
            self.logger.error(f"{_f_name}: Error processing parameter request: {str(e)}", exc_info=True)
            param_value_received = None
        
        except Exception as e:
            self.logger.error(f"{_f_name}: Unexpected error during parameter request: {str(e)}", exc_info=True)
            param_value_received = None
        
        finally:
            self._end_operation() 
            self._clear_queues(clear_operation=True)
            return param_value_received

    def set_param(self, param_id_str: Optional[str] = None, value_to_set: Optional[float] = None,
                                mav_param_type: Optional[int] = None, item_timeout: float = 1.0, retries: int = 3) -> bool:
        """
        Internal: Sends PARAM_SET and waits for PARAM_VALUE confirmation.
        Assumes operation lock is already held.
        """
        
        _f_name = self.__class__.__name__ + '.' + self.set_param.__name__
        
        if not isinstance(param_id_str, str):
            self.logger.error(f"{_f_name}: Invalid parameter ID: {param_id_str}. Expected str.")
            return False
        
        if not isinstance(value_to_set, (int, float)):
            self.logger.error(f"{_f_name}: Invalid value to set: {value_to_set}. Expected int or float.")
            return False
        
        if not isinstance(mav_param_type, int):
            self.logger.error(f"{_f_name}: Invalid MAVLink parameter type: {mav_param_type}. Expected int.")
            return False
        
        if not mav_param_type in [self.MAV_PARAM_TYPE_REAL32, self.MAV_PARAM_TYPE_UINT8, self.MAV_PARAM_TYPE_INT8,
                                      self.MAV_PARAM_TYPE_UINT16, self.MAV_PARAM_TYPE_INT16,self.MAV_PARAM_TYPE_UINT32, self.MAV_PARAM_TYPE_INT32]:
            self.logger.error(f"{_f_name}: Invalid mav_param_type value: {mav_param_type}. Must be a valid MAVLink parameter type.")
            return False
        
        if not self._is_ready(): return False
        if not self._start_operation(): return False
        
        self.logger.info(f"{_f_name}: Setting parameter '{param_id_str}' to {value_to_set} (Type: {mav_param_type})...")
        self._clear_queues(clear_operation=True)
        confirmed = False
        
        try:
            self.conn.mav.param_set_send(
                self.target_system,
                self.target_component,
                param_id_str, 
                float(value_to_set),
                mav_param_type
            )

            response_msg = self._wait_for_message(
                'PARAM_VALUE', 
                item_timeout,
                retries,
                _f_name
            )

            if response_msg:
                received_param_id = response_msg.get('param_id')
                received_value = response_msg.get('param_value')

                if received_param_id == param_id_str:
                    if received_value is not None:
                        if math.isclose(received_value, value_to_set, rel_tol=1e-5, abs_tol=1e-7):
                            self.logger.info(f"{_f_name}: Parameter '{param_id_str}' set successfully to {received_value}.")
                            confirmed = True
                        else:
                            self.logger.error(f"{_f_name}: Parameter '{param_id_str}' set to {received_value}, but expected {value_to_set}.")
                    else:
                        self.logger.error(f"{_f_name}: PARAM_VALUE for '{param_id_str}' received, but 'param_value' field is None.")
                else:
                    self.logger.warning(f"{_f_name}: Received PARAM_VALUE for different ID: '{received_param_id}' (expected '{param_id_str}').")
        except Exception as e:
            self.logger.error(f"{_f_name}: Error during parameter set: {str(e)}", exc_info=True)
        finally:
            self._end_operation() 
            self._clear_queues(clear_operation=True)
            return confirmed

    def get_param_list(self, item_timeout: float = 1.0, retries: int = 3) -> Optional[Dict[str, float]]:
        """
        Internal: Sends PARAM_REQUEST_LIST and collects PARAM_VALUE messages.
        Assumes operation lock is held.
        """
        
        _f_name = self.__class__.__name__ + '.' + self.get_param_list.__name__
        
        if not self._is_ready(): return None
        if not self._start_operation(): return None
        
        self.logger.info(f"{_f_name}: Starting parameter list download...")
        self._clear_queues(clear_operation=True)
        param_list = {}
        
        try:
            self.conn.mav.param_request_list_send(
                self.target_system,
                self.target_component
            )

            while True:
                response_msg = self._wait_for_message(
                    'PARAM_VALUE', 
                    item_timeout,
                    retries,
                    _f_name
                )

                if response_msg:
                    param_id = response_msg.get('param_id')
                    param_value = response_msg.get('param_value')
                    if param_id and param_value is not None:
                        param_list[param_id] = param_value
                        self.logger.debug(f"{_f_name}: Received PARAM_VALUE for '{param_id}': {param_value}")
                    else:
                        self.logger.warning(f"{_f_name}: PARAM_VALUE received with missing ID or value.")
                else:
                    break
            
            return param_list if len(param_list) > 0 else None
        
        except (ValueError, TypeError) as e:
            self.logger.error(f"{_f_name}: Error processing parameter list: {str(e)}", exc_info=True)
            return None
        
        except Exception as e:
            self.logger.error(f"{_f_name}: Error during parameter list download: {str(e)}", exc_info=True)
            return None
        
        finally:
            self._end_operation() 
            self._clear_queues(clear_operation=True)

    def _send_position_target_local_ned(self,
                                        coordinate_frame: int,
                                        type_mask: int,
                                        x: float, y: float, z: float,
                                        vx: float, vy: float, vz: float,
                                        afx: float, afy: float, afz: float,
                                        yaw: float, yaw_rate: float) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self._send_position_target_local_ned.__name__
        try:
            self.conn.mav.set_position_target_local_ned_send(
                0, self.target_system, self.target_component,
                coordinate_frame, type_mask, x, y, z, vx, vy, vz, afx, afy, afz, yaw, yaw_rate
            )
            self.logger.debug(f"{_f_name}: SET_POSITION_TARGET_LOCAL_NED sent successfully.")
            return True
        
        except Exception as e:
            self.logger.error(f"{_f_name}: Failed to send SET_POSITION_TARGET_LOCAL_NED: {e}", exc_info=True)
            return False