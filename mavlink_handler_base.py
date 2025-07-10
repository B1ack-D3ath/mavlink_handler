import logging
import math 
from typing import Optional, List, Dict, Union, Any
from .mavlink_handler_core import MAVLinkHandlerCore
from pymavlink import mavutil

class MAVLinkHandlerBase(MAVLinkHandlerCore):
    """
    Provides higher-level, vehicle-agnostic functions built upon the modified MAVLinkHandlerCore.
    Includes helper for ensuring armed state and getters for common vehicle states.
    Parameter dictionaries for commands only include explicitly required values.
    """

    # --- MAVLink Command IDs ---
    CMD_DO_MOUNT_CONTROL = mavutil.mavlink.MAV_CMD_DO_MOUNT_CONTROL
    CMD_DO_SET_ROI = mavutil.mavlink.MAV_CMD_DO_SET_ROI
    CMD_MISSION_START = mavutil.mavlink.MAV_CMD_MISSION_START
    CMD_COMPONENT_ARM_DISARM = mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM
    CMD_DO_REPOSITION = mavutil.mavlink.MAV_CMD_DO_REPOSITION
    CMD_PREFLIGHT_REBOOT_SHUTDOWN = mavutil.mavlink.MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN
    CMD_DO_CHANGE_SPEED = mavutil.mavlink.MAV_CMD_DO_CHANGE_SPEED
    CMD_NAV_RETURN_TO_LAUNCH = mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH # NEW
    
    def __init__(self, mavlink_url: str = "udp:localhost:14550", baud: int = 57600, source_system: int = 255, logger: Optional[logging.Logger] = None):
        """Initializes the MAVLinkHandlerBase."""
        self.logger = logger if isinstance(logger, logging.Logger) else logging.getLogger(__name__)
        super().__init__(mavlink_url=mavlink_url, baud=baud, source_system=source_system, logger=logger)
        _f_name = self.__class__.__name__ + self.__init__.__name__
        self.logger.info(f"{_f_name}: Initializing MAVLinkHandlerBase...")


    # --- Pre-condition Check Helpers ---
    def _ensure_armed(self, function_name: str = 'Unknown', attempt_arm: bool = True, force: bool = False, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        
        if not isinstance(function_name, str): function_name = 'Unknown'
        _f_name = self.__class__.__name__ + self._ensure_armed.__name__ + function_name
        
        if self._is_armed():
            self.logger.info(f"{_f_name}: Vehicle is already armed.")
            return True
        
        if not isinstance(attempt_arm, bool): attempt_arm = True
        
        self.logger.warning(f"{_f_name}: Vehicle is not armed.")
        
        if attempt_arm:
            self.logger.info(f"{_f_name}: Attempting to arm vehicle...")
            
            if self.arm_disarm(attempt_arm, force, item_timeout, retries):
                self.logger.info(f"{_f_name}: Vehicle armed successfully.")
                return True
            
            self.logger.error(f"{_f_name}: Failed to arm vehicle.")
            return False
        
        self.logger.error(f"{_f_name}: Vehicle is not armed and attempt_arm=False.")
        return False

    # --- Common Vehicle Commands ---
    def send_gimbal_control(self, pitch_deg: float = 0.0, roll_deg: float = 0.0, yaw_deg: float = 0.0, item_timout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        
        _f_name = self.__class__.__name__ + self.send_gimbal_control.__name__
        
        if not isinstance(pitch_deg, (float, int)) or -90 <= pitch_deg <= 90:
            self.logger.error(f"{_f_name}: Invalid pitch_deg value.")
            return False
        
        if not isinstance(roll_deg, (float, int)) or -90 <= roll_deg <= 90:
            self.logger.error(f"{_f_name}: Invalid roll_deg value.")
            return False
        
        if not isinstance(yaw_deg, (float, int)) or -180 <= yaw_deg <= 180:
            self.logger.error(f"{_f_name}: Invalid yaw_deg value.")
            return False
        
        self.logger.info(f"Sending Gimbal Control: Pitch={pitch_deg}, Roll={roll_deg}, Yaw={yaw_deg}")
        mount_mode = mavutil.mavlink.MAV_MOUNT_MODE_MAVLINK_TARGETING
        command_data = {
            "type": "COMMAND_LONG", "command": self.CMD_DO_MOUNT_CONTROL,
            "param1": float(pitch_deg), "param2": float(roll_deg), "param3": float(yaw_deg),
            "param7": float(mount_mode),
        }
        
        if self.send_command(command_data, item_timout, retries):
            self.logger.info(f"{_f_name}: Command acknowledged successfully.")
            return True
        
        self.logger.error(f"{_f_name}: Command failed or was rejected.")
        return False

    def set_roi(self, lat: Optional[float] = None, lon: Optional[float] = None, alt_m: Optional[float] = None, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.set_roi.__name__
        
        if not isinstance(lat, (float, int)) or not isinstance(lon, (float, int)) or not isinstance(alt_m, (float, int)) or (lat == 0 and lon == 0 and alt_m == 0): 
            mode = mavutil.mavlink.MAV_ROI_NONE
            self.logger.info("Resetting ROI (MAV_ROI_NONE).")
            command_data = {
                "type": "COMMAND_INT", "command": self.CMD_DO_SET_ROI,
                "param1": float(mode), "frame": mavutil.mavlink.MAV_FRAME_GLOBAL,
            }
        else:
            mode = mavutil.mavlink.MAV_ROI_LOCATION
            try:
                lat_int = int(lat * 1e7); lon_int = int(lon * 1e7); alt_float = float(alt_m)
                self.logger.info(f"{_f_name}: Lat={lat}, Lon={lon}, Alt={alt_float}m MSL")
            except (TypeError, ValueError) as e:
                self.logger.error(f"{_f_name}: lat={lat}, lon={lon}, alt={alt_m}. Error: {e}")
                return False
            command_data = {
                "type": "COMMAND_INT", "command": self.CMD_DO_SET_ROI,
                "frame": mavutil.mavlink.MAV_FRAME_GLOBAL, "param1": float(mode),
                "param5": lat_int, "param6": lon_int, "param7": alt_float
            }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: Command acknowledged successfully.")
            return True
        
        self.logger.error(f"{_f_name}: Command failed or was rejected.")
        return False

    def mission_start(self, first_item: int = 0, last_item: int = 0, attempt_arm: bool = True, arm_timeout_sec: int = 5, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.mission_start.__name__
        
        if not self._ensure_armed(_f_name, attempt_arm, arm_timeout_sec, item_timeout, retries):
            self.logger.error(f"{_f_name}: Vehicle is not armed. Cannot start mission.")
            return False
        self.logger.info(f"{_f_name}: Attempting to start mission with first_item={first_item}, last_item={last_item}...")
        command_data = {
            "type": "COMMAND_LONG", "command": self.CMD_MISSION_START,
            "param1": float(first_item), "param2": float(last_item),
        }
        
        if self.send_command(command_data, item_timeout, retries,):
            self.logger.info(f"{_f_name}: Command acknowledged successfully.")
            return True
        
        self.logger.error(f"{_f_name}: Command failed or was rejected.")
        return False

    def mission_clear(self, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.mission_clear.__name__
        
        self.logger.info(f"{_f_name}: Attempting to clear mission...")
        if self.mission_set([], item_timeout, retries):
            self.logger.info(f"{_f_name}: Mission cleared successfully.")
            return True
        self.logger.error(f"{_f_name}: Failed to clear mission.")
        return False

    def send_goto(self, latitude: Optional[float] = None, longitude: Optional[float] = None,
                  altitude_rel: Optional[float] = None, speed: Optional[float] = None, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.send_goto.__name__
        # Note: Pre-condition checks are expected in subclass overrides.
        target_lat = latitude
        target_lon = longitude
        target_alt_rel = altitude_rel
        target_speed = speed if speed is not None else -1.0 
        if target_lat is None or target_lon is None or target_alt_rel is None:
            current_pos = self.get_position() 
            if current_pos is None:
                self.logger.error(f"{_f_name}: Current position unavailable.")
                return False
            if target_lat is None:
                if current_pos.get('lat') is None:
                    self.logger.error(f"{_f_name}: Current latitude unavailable.")
                    return False
                target_lat = current_pos.get('lat')
            if target_lon is None:
                if current_pos.get('lon') is None:
                    self.logger.error(f"{_f_name}: Current longitude unavailable.")
                    return False
                target_lon = current_pos.get('lon')
            if target_alt_rel is None:
                if current_pos.get('alt_rel_m') is None:
                    self.logger.error(f"{_f_name}: Current relative altitude unavailable.")
                    return False
                target_alt_rel = current_pos.get('alt_rel_m')
        self.logger.info(f"{_f_name}: Sending GOTO command to Lat={target_lat}, Lon={target_lon}, Alt={target_alt_rel}m, Speed={target_speed}m/s")
        command_data = {
            "type": "COMMAND_INT", "command": self.CMD_DO_REPOSITION,
            "param1": float(target_speed), 
            "param2": float(mavutil.mavlink.MAV_DO_REPOSITION_FLAGS_CHANGE_MODE), 
            "param5": int(target_lat * 1e7), "param6": int(target_lon * 1e7), "param7": float(target_alt_rel),    
        }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: Command acknowledged successfully.")
            return True
        
        self.logger.error(f"{_f_name}: Command failed or was rejected.")
        return False

    def reboot_autopilot(self, shutdown: bool = False, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.reboot_autopilot.__name__
        
        if not isinstance(shutdown, bool): shutdown = False
        
        action_str = "shutdown" if shutdown else "reboot"
        param1_val = 3.0 if shutdown else 1.0 
        self.logger.warning(f"{_f_name}: Attempting to {action_str} autopilot...")
        self.logger.warning(f"{_f_name}: This may take a few seconds. Please wait...")
        command_data = {
            "type": "COMMAND_LONG", "command": self.CMD_PREFLIGHT_REBOOT_SHUTDOWN,
            "param1": param1_val, "param2": 0.0,
        }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.error(f"{_f_name}: Command acknowledged successfully. Autopilot will {action_str} shortly.")
            return True
        
        self.logger.info(f"{_f_name}: Command failed or was rejected.")
        return False

    def set_target_speed(self, speed_type: int = 0, speed_value: float = 0.0, throttle_percent: int = -1, relative: bool = False, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.set_target_speed.__name__
        
        if not isinstance(speed_type, int) or speed_type not in [0, 1, 2, 3]:
            self.logger.error(f"{_f_name}: Invalid speed_type {speed_type}. Must be 0 (ground), 1 (air), 2 (ground + air), or 3 (ground + air + throttle).")
            return False
        
        if not isinstance(speed_value, (float, int)):
            self.logger.error(f"{_f_name}: Invalid speed_value {speed_value}. Must be a float or int.")
            return False
        
        if speed_value < 0 and not math.isclose(speed_value, -1.0) and speed_type in [0,1]:
                self.logger.warning(f"{_f_name}: Speed_value {speed_value} is negative but not -1 (no change). Behavior might be undefined for Air/Ground speed.")
        
        if not isinstance(throttle_percent, int) or not (-1 <= throttle_percent <= 100):
            self.logger.error(f"{_f_name}: Invalid throttle_percent {throttle_percent}. Must be between -1 and 100.")
            return False
        
        if not isinstance(relative, bool): 
            self.logger.error(f"{_f_name}: Invalid relative value {relative}. Must be a boolean.")
            return False
        
        param1_speed_type = float(speed_type)
        param2_speed_value_ms = float(speed_value)
        param3_throttle_percent = float(throttle_percent)
        param4_relative = 1.0 if relative else 0.0 
        self.logger.info(f"{_f_name}: Sending DO_CHANGE_SPEED command with speed_type={param1_speed_type}, speed_value={param2_speed_value_ms}, throttle_percent={param3_throttle_percent}, relative={param4_relative}")
        command_data = {
            "type": "COMMAND_LONG", "command": self.CMD_DO_CHANGE_SPEED,
            "param1": param1_speed_type, "param2": param2_speed_value_ms,
            "param3": param3_throttle_percent, "param4": param4_relative
        }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: Command acknowledged successfully.")
            return True
        
        self.logger.error(f"{_f_name}: Command failed or was rejected.")
        return False
    
            
    # --- NEW: Return to Launch ---
    def send_return_to_launch(self, item_timeout: float = 1.0, retries: int = 3) -> bool:
        """
        Commands the vehicle to initiate Return-to-Launch (RTL) mode.

        This command typically requires the vehicle to be armed and have a valid
        home position (GPS lock). The specific RTL behavior (altitude, landing, etc.)
        depends on vehicle parameters.

        Returns:
            bool: True if the command was acknowledged successfully, False otherwise.
                  Note: Success only means the command was received, not that the
                  vehicle is in RTL mode or has completed the return.
        """
        _f_name = self.__class__.__name__ + self.send_return_to_launch.__name__

        # Pre-check: Ensure vehicle is armed. RTL usually doesn't make sense otherwise.
        # We won't attempt to arm here; the caller should ensure the vehicle is operational.
        if not self._is_armed():
            self.logger.error(f"{_f_name}: Vehicle is not armed. Cannot send RTL command.")
            return False
        
        # Optional Pre-check: GPS fix? RTL relies on home position.
        gps_data = self.get_gps_status_raw()
        if gps_data is None or gps_data.get('fix_type', 0) < 2: # 0-1: No fix, 2: 2D, 3: 3D
            self.logger.warning(f"{_f_name}: GPS fix not available or insufficient. RTL may not work as expected.")
            # Decide whether to return False or proceed with warning. Let's proceed with warning for now.

        self.logger.info
        command_data = {
            "type": "COMMAND_LONG",
            "command": self.CMD_NAV_RETURN_TO_LAUNCH,
            # All params are typically 0 for this command
        }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: NAV_RETURN_TO_LAUNCH command acknowledged successfully.")
            return True
        
        self.logger.error(f"{_f_name}: NAV_RETURN_TO_LAUNCH command failed or was rejected.")
        return False

    # --- State Getters ---
    def get_latitude(self) -> Optional[float]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            pos_data = self._last_global_position_int
            if pos_data is None: return None
            try: return pos_data.get('lat', 0) / 1e7
            except (TypeError, KeyError): return None

    def get_longitude(self) -> Optional[float]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            pos_data = self._last_global_position_int
            if pos_data is None: return None
            try: return pos_data.get('lon', 0) / 1e7
            except (TypeError, KeyError): return None

    def get_relative_altitude(self) -> Optional[float]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            pos_data = self._last_global_position_int
            if pos_data is None: return None
            try: return pos_data.get('relative_alt', 0) / 1000.0
            except (TypeError, KeyError): return None

    def get_msl_altitude(self) -> Optional[float]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            pos_data = self._last_global_position_int
            if pos_data is None: return None
            try: return pos_data.get('alt', 0) / 1000.0
            except (TypeError, KeyError): return None

    def get_heading(self) -> Optional[float]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            pos_data = self._last_global_position_int
            if pos_data is None: return None
            try:
                hdg_cdeg = pos_data.get('hdg')
                if hdg_cdeg is None or hdg_cdeg == 65535: return None
                return hdg_cdeg / 100.0
            except (TypeError, KeyError): return None

    def get_battery_status(self) -> Optional[Dict[str, Any]]:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.get_battery_status.__name__
        
        with self._state_lock:
            batt_data = self._last_battery_status
            if batt_data is None:
                 self.logger.error(f"{_f_name}: Battery status data is None.")
                 return None
            try:
                voltages_mv = batt_data.get('voltages', []) or [] 
                current_ca = batt_data.get('current_battery', -1) 
                remaining_percent = batt_data.get('battery_remaining', -1) 
                battery_id = batt_data.get('id', 0)
                time_ms = batt_data.get('time_boot_ms')
                formatted_status = {
                    'voltages_mv': voltages_mv,
                    'current_ma': current_ca * 10 if current_ca is not None and current_ca != -1 else None, 
                    'remaining_percent': remaining_percent if remaining_percent is not None and remaining_percent != -1 else None,
                    'id': battery_id, 'time_boot_ms': time_ms
                }
                valid_voltages = [v for v in voltages_mv if v is not None and v != 65535 and v != 0xFFFF]
                formatted_status['total_voltage_v'] = sum(valid_voltages) / 1000.0 if valid_voltages else None
                return formatted_status
            except Exception as e:
                self.logger.error(f"{_f_name}: Error processing battery status data: {e}", exc_info=True)
                return None


    def get_attitude(self) -> Optional[Dict[str, Any]]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            attitude_data = self._last_attitude
            if attitude_data is None: return None
            return attitude_data.copy() 

    def get_local_position_ned(self) -> Optional[Dict[str, Any]]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            local_pos_data = self._last_local_position_ned
            if local_pos_data is None: return None
            return local_pos_data.copy()

    def get_gps_status_raw(self) -> Optional[Dict[str, Any]]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            gps_data = self._last_gps_raw_int
            if gps_data is None: return None
            return gps_data.copy()

    def get_vfr_hud_data(self) -> Optional[Dict[str, Any]]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            vfr_data = self._last_vfr_hud
            if vfr_data is None: return None
            return vfr_data.copy()

    def get_landed_state_id(self) -> Optional[int]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            ext_state = self._last_extended_sys_state
            if ext_state is None: return None
            return ext_state.get('landed_state')

    def get_airspeed(self) -> Optional[float]:
        # ... (implementation remains the same) ...
        with self._state_lock:  
            vfr_data = self._last_vfr_hud  
            if vfr_data is None: return None
            airspeed_value = vfr_data.get('airspeed') 
            if airspeed_value is None: return None
            if not isinstance(airspeed_value, (float, int)): return None
        return float(airspeed_value)

    # --- Dynamic Stream Rate Control ---
    def set_message_stream_rate(self, msg_id_or_name: Union[int, str], frequency_hz: int, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.set_message_stream_rate.__name__
        
        actual_msg_id = -1
        if isinstance(msg_id_or_name, int): actual_msg_id = msg_id_or_name
        elif isinstance(msg_id_or_name, str):
            msg_name_upper = msg_id_or_name.upper()
            if hasattr(mavutil.mavlink, 'name_to_id'):
                actual_msg_id = mavutil.mavlink.name_to_id.get(msg_name_upper, -1)
            
            if actual_msg_id == -1: 
                potential_attr_name = f"{msg_name_upper}_TYPE_ID" 
                if hasattr(MAVLinkHandlerCore, potential_attr_name): 
                    attr_val = getattr(MAVLinkHandlerCore, potential_attr_name, -1)
                    if isinstance(attr_val, int): actual_msg_id = attr_val
                
                if actual_msg_id == -1:
                    self.logger.error(f"{_f_name}: Unknown message name: '{msg_id_or_name}'.")
                    return False
        
        else:
            self.logger.error(f"{_f_name}: Invalid msg_id_or_name type: {type(msg_id_or_name)}.")
            return False
        
        if not isinstance(frequency_hz, int):
            self.logger.error(f"{_f_name}: Invalid frequency_hz type: {type(frequency_hz)}. Must be an integer.")
            return False
        
        self.logger.info(f"{_f_name}: Setting stream rate for message ID {actual_msg_id} to {frequency_hz} Hz.")
        rates_to_set: Dict[int, int] = {actual_msg_id: frequency_hz}
        return self.set_streamrates(rates_to_set, item_timeout, retries)


    def set_message_stream_rates_config(self, rates_config: Dict[Union[int, str], int], item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.set_message_stream_rates_config.__name__
        
        if not isinstance(rates_config, dict):
            self.logger.error(f"{_f_name}: Invalid rates_config type: {type(rates_config)}. Must be a dictionary.")
            return False
        
        if not rates_config: return True 
        resolved_rates_hz: Dict[int, int] = {}
        all_keys_valid = True
        
        self.logger.info(f"{_f_name}: Processing stream rates configuration...")
        
        for msg_id_or_name, freq_hz in rates_config.items():
            actual_msg_id = -1
            current_key_str = str(msg_id_or_name) 
            if isinstance(msg_id_or_name, int): actual_msg_id = msg_id_or_name
            
            elif isinstance(msg_id_or_name, str):
                msg_name_upper = msg_id_or_name.upper()
                if hasattr(mavutil.mavlink, 'name_to_id'): actual_msg_id = mavutil.mavlink.name_to_id.get(msg_name_upper, -1)
                
                if actual_msg_id == -1: 
                    potential_attr_name = f"{msg_name_upper}_TYPE_ID"
                    
                    if hasattr(MAVLinkHandlerCore, potential_attr_name): 
                        attr_val = getattr(MAVLinkHandlerCore, potential_attr_name, -1)
                        if isinstance(attr_val, int): actual_msg_id = attr_val
                
                if actual_msg_id == -1:
                    self.logger.error(f"{_f_name}: Unknown message name: '{msg_id_or_name}'.")
                    all_keys_valid = False
                    break
            
            else: 
                self.logger.error(f"{_f_name}: Invalid msg_id_or_name type: {type(msg_id_or_name)}.")
                all_keys_valid = False
                break
            
            if not isinstance(freq_hz, int):
                self.logger.error(f"{_f_name}: Invalid frequency_hz type: {type(freq_hz)}. Must be an integer.")
                all_keys_valid = False
                break
            
            resolved_rates_hz[actual_msg_id] = freq_hz
        
        if not all_keys_valid: return False 
        if not resolved_rates_hz:
            self.logger.warning(f"{_f_name}: No valid message IDs found in rates_config. No changes made.")
            return False if rates_config else True
        
        self.logger.info(f"{_f_name}: Setting stream rates for {len(resolved_rates_hz)} messages...")
        return self.set_streamrates(resolved_rates_hz, item_timeout, retries)

    # --- Common Arm/Disarm ---
    def arm_disarm(self, do_arm: bool = False, force: bool = False, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.arm_disarm.__name__
        
        if not isinstance(do_arm, bool): do_arm = False
        if not isinstance(force, bool): force = False
        
        self.logger.info(f"{_f_name}: Attempting to {'arm' if do_arm else 'disarm'} vehicle...")
        
        if do_arm and self._is_armed():
            self.logger.info(f"{_f_name}: Vehicle is already armed.")
            return True
        
        if do_arm and force:
            self.logger.warning(f"{_f_name}: Force arm requested!")
        
        if not do_arm and not self._is_armed():
            self.logger.info(f"{_f_name}: Vehicle is already disarmed.")
            return True
        
        if not do_arm and force:
            self.logger.info(f"{_f_name}: Force disarm requested!")
        
        command_data = {
            "type": "COMMAND_LONG", "command": self.CMD_COMPONENT_ARM_DISARM,
            "param1": 1.0 if do_arm else 0.0, "param2": 21196.0 if force else 0.0,
        }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: Command acknowledged successfully.")
            return True
        
        self.logger.error(f"{_f_name}: Command failed or was rejected by ACK.")
        return False

    def set_param_list(self, params_to_set: Dict[str, float] = None, param_type_mavlink: Optional[int] = None, item_timeout: float = 1.0, retries: int = 3)-> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.set_param_list.__name__
        
        if not isinstance(params_to_set, dict):
            self.logger.error(f"{_f_name}: Invalid params_to_set type: {type(params_to_set)}. Must be a dictionary.")
            return False
        
        self.logger.info(f"{_f_name}: Setting {len(params_to_set)} parameters...")
        for param_name, param_value in params_to_set.items():
            if not isinstance(param_name, str):
                self.logger.error(f"{_f_name}: Invalid parameter name type: {type(param_name)}. Must be a string.")
                return False
            
            if not isinstance(param_value, (float, int)):
                self.logger.error(f"{_f_name}: Invalid parameter value type: {type(param_value)}. Must be a float or int.")
                return False
            
            if not self.set_param(param_name, param_value, param_type_mavlink, item_timeout, retries):
                self.logger.error(f"{_f_name}: Failed to set parameter '{param_name}' to {param_value}.")
                return False
            self.logger.debug(f"{_f_name}: Parameter '{param_name}' set to {param_value}.")
        
        self.logger.info(f"{_f_name}: All parameters set successfully.")
        return True