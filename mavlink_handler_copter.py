import logging
import math
from typing import Optional, Dict
from .mavlink_handler_base import MAVLinkHandlerBase
from pymavlink import mavutil

class MAVLinkHandlerCopter(MAVLinkHandlerBase):
    """
    Provides Copter-specific functionalities, inheriting from MAVLinkHandlerBase.
    Includes methods for Copter takeoff, land, setting specific modes,
    interpreting Copter flight modes, and sending refined guided mode targets.
    Overrides send_goto to add pre-checks.
    Uses helper functions for mode and arm status checks.
    """

    # --- Copter-Specific MAVLink Command IDs ---
    CMD_NAV_TAKEOFF = mavutil.mavlink.MAV_CMD_NAV_TAKEOFF
    CMD_NAV_LAND = mavutil.mavlink.MAV_CMD_NAV_LAND
    CMD_DO_SET_MODE = mavutil.mavlink.MAV_CMD_DO_SET_MODE
    
    MAV_MODE_FLAG_CUSTOM_MODE_ENABLED = mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED
    MAV_MODE_FLAG_MANUAL_INPUT_ENABLED = mavutil.mavlink.MAV_MODE_FLAG_MANUAL_INPUT_ENABLED
    
    POSITION_TARGET_TYPEMASK_X_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_X_IGNORE
    POSITION_TARGET_TYPEMASK_Y_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_Y_IGNORE
    POSITION_TARGET_TYPEMASK_Z_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_Z_IGNORE
    
    POSITION_TARGET_TYPEMASK_VX_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_VX_IGNORE
    POSITION_TARGET_TYPEMASK_VY_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_VY_IGNORE
    POSITION_TARGET_TYPEMASK_VZ_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_VZ_IGNORE
    
    POSITION_TARGET_TYPEMASK_AX_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_AX_IGNORE
    POSITION_TARGET_TYPEMASK_AY_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_AY_IGNORE
    POSITION_TARGET_TYPEMASK_AZ_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_AZ_IGNORE
    
    #POSITION_TARGET_TYPEMASK_YAW_ANGLE_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_YAW_ANGLE_IGNORE
    POSITION_TARGET_TYPEMASK_YAW_RATE_IGNORE = mavutil.mavlink.POSITION_TARGET_TYPEMASK_YAW_RATE_IGNORE
    POSITION_TARGET_TYPEMASK_FORCE_SET = mavutil.mavlink.POSITION_TARGET_TYPEMASK_FORCE_SET
    
    MAV_FRAME_LOCAL_NED = mavutil.mavlink.MAV_FRAME_LOCAL_NED
    MAV_FRAME_LOCAL_OFFSET_NED = mavutil.mavlink.MAV_FRAME_LOCAL_OFFSET_NED

    # --- ArduCopter Custom Flight Modes ---
    # Note: Verify this list against your target ArduCopter version documentation
    COPTER_MODES: Dict[str, int] = {
        "STABILIZE": 0, "ACRO": 1, "ALT_HOLD": 2, "AUTO": 3, "GUIDED": 4,
        "LOITER": 5, "RTL": 6, "CIRCLE": 7, "POSITION": 8, "LAND": 9,
        "DRIFT": 11, "SPORT": 13, "FLIP": 14, "AUTOTUNE": 15, "POSHOLD": 16,
        "BRAKE": 17, "THROW": 18, "AVOID_ADSB": 19, "GUIDED_NOGPS": 20,
        "SMART_RTL": 21, "FLOWHOLD": 22, "FOLLOW": 23, "ZIGZAG": 24,
        "SYSTEMID": 25, "AUTOROTATE": 26, "TURTLE": 27,
        # "NEW_SIMPLE": 26, # Consider removing if Simple mode is not set via custom mode ID
    }
    COPTER_MODE_NAMES: Dict[int, str] = {v: k for k, v in COPTER_MODES.items()}


    def __init__(self, mavlink_url: str = "udp:localhost:14550", baud: int = 57600, source_system: int = 255, logger: Optional[logging.Logger] = None):
        """Initializes the MAVLinkHandlerCopter."""
        
        self.logger = logger if isinstance(logger, logging.Logger) else logging.getLogger(__name__)
        super().__init__(mavlink_url=mavlink_url, baud=baud, source_system=source_system, logger=logger)
        _f_name = self.__class__.__name__ + self.__init__.__name__
        self.logger.info(f"{_f_name}: Initializing MAVLinkHandlerCopter...")


    # --- Copter Specific Pre-condition Check Helpers ---

    def _ensure_mode(self, required_mode: Optional[str] = None, function_name: str = "Unknown",
                    attempt_set_mode: bool = True, item_timeout: float = 1.0, retries: int = 3) -> bool:
        
        if not isinstance(function_name, str): function_name = "Unknown"
        _f_name = self.__class__.__name__ + self._ensure_mode.__name__ + function_name
        
        if not isinstance(required_mode, str):
            self.logger.error(f"{_f_name}: Required mode must be a string. Received: {type(required_mode)}")
            return False
        
        required_mode_upper = required_mode.upper()
        if required_mode_upper not in self.COPTER_MODES:
            self.logger.error(f"{_f_name}: Invalid Copter mode name: '{required_mode_upper}'. Valid: {list(self.COPTER_MODES.keys())}")
            return False
        
        current_mode = self.get_flight_mode()
        
        if current_mode is None:
            self.logger.error(f"{_f_name}: Unable to determine current flight mode.")
            current_mode = "UNKNOWN"
        
        if current_mode == required_mode_upper: 
            self.logger.info(f"{_f_name}: Vehicle already in {required_mode_upper} mode.")
            return True
        
        if not isinstance(attempt_set_mode, bool): attempt_set_mode = True
        
        self.logger.warning(f"{_f_name}: Vehicle not in {required_mode_upper} mode (Current: {current_mode}).")
        
        if attempt_set_mode:
            self.logger.info(f"{_f_name}: Attempting to set {required_mode_upper} mode...")
            
            if self.set_mode(required_mode_upper, item_timeout, retries):
                self.logger.info(f"{_f_name}: Set mode to {required_mode_upper} command acknowledged.")
                return True
            
            self.logger.error(f"{_f_name}: Failed to set {required_mode_upper} mode.")
            return False
        
        self.logger.error(f"{_f_name}: Vehicle not in {required_mode_upper} mode (Current: {current_mode}).")
        return False

    # --- Copter Specific Commands (Using Helpers) ---

    def takeoff(self, altitude_m: Optional[float] = None, attempt_set_guided: bool = True, attempt_arm: bool = True, force: bool = False,
                item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.takeoff.__name__
        
        if not isinstance(altitude_m, (int, float)):
            self.logger.error(f"{_f_name}: Takeoff altitude must be a number. Received: {type(altitude_m)}")
            return False
        
        if altitude_m <= 0:
            self.logger.error(f"{_f_name}: Takeoff altitude must be positive. Received: {altitude_m}")
            return False
        
        if not self._ensure_mode("GUIDED", _f_name, attempt_set_guided, item_timeout, retries):
            self.logger.error(f"{_f_name}: Vehicle must be in GUIDED mode.")
            return False
        
        if not self._ensure_armed(_f_name, attempt_arm, False, item_timeout, retries):
            self.logger.error(f"{_f_name}: Vehicle must be armed.")
            return False
        
        self.logger.info(f"{_f_name}: Sending Takeoff command to {altitude_m:.2f}m.")
        command_data = {type: "COMMAND_LONG", "command": self.CMD_NAV_TAKEOFF, "param7": float(altitude_m)}
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: Takeoff command acknowledged.")
            return True
        
        self.logger.error(f"{_f_name}: Takeoff command failed or was rejected.")
        return False

    def land(self, attempt_set_guided: bool = True, attempt_arm: bool = False, item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.land.__name__
        
        if not self._ensure_mode("GUIDED", _f_name, attempt_set_guided, item_timeout, retries):
            self.logger.error(f"{_f_name}: Vehicle must be in GUIDED mode.")
            return False
        
        if not self._ensure_armed(_f_name, attempt_arm, False, item_timeout, retries):
            self.logger.error(f"{_f_name}: Vehicle must be armed.")
            return False
        
        self.logger.info(f"{_f_name}: Sending Land command (MAV_CMD_NAV_LAND)...")
        command_data = { "type": "COMMAND_LONG", "command": self.CMD_NAV_LAND }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: Land command acknowledged.")
            return True
        
        self.logger.error(f"{_f_name}: Land command failed or was rejected.")
        return False

    def set_mode(self, mode_name: str = "Unknown", item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.set_mode.__name__
        
        if not isinstance(mode_name, str):
            self.logger.error(f"{_f_name}: Required mode must be a string. Received: {type(mode_name)}")
            return False
        
        mode_name_upper = mode_name.upper()
        if mode_name_upper not in self.COPTER_MODES:
            self.logger.error(f"{_f_name}: Invalid Copter mode name: '{mode_name_upper}'. Valid: {list(self.COPTER_MODES.keys())}")
            return False
        
        current_mode = self.get_flight_mode()
        
        if current_mode is None:
            self.logger.error(f"{_f_name}: Unable to determine current flight mode.")
            current_mode = "UNKNOWN"
        
        if current_mode == mode_name_upper: 
            self.logger.info(f"{_f_name}: Vehicle already in {mode_name_upper} mode.")
            return True
        
        target_mode_id = self.COPTER_MODES[mode_name_upper]
        self.logger.info(f"{_f_name}: Setting mode to {mode_name_upper} (ID: {target_mode_id})...")
        
        command_data = {
            "type": "COMMAND_LONG", "command": self.CMD_DO_SET_MODE,
            "param1": float(self.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED),
            "param2": float(target_mode_id),
        }
        
        if self.send_command(command_data, item_timeout, retries):
            self.logger.info(f"{_f_name}: Set mode command acknowledged.")
            return True
        
        self.logger.error(f"{_f_name}: Set mode command failed or was rejected.")
        return False

    # --- Overridden send_goto with Pre-checks ---
    def send_goto(self, latitude: Optional[float] = None, longitude: Optional[float] = None,
                altitude_rel: Optional[float] = None, speed: Optional[float] = None,
                attempt_set_guided: bool = False, attempt_arm: bool = False,
                item_timeout: float = 1.0, retries: int = 3) -> bool:
        # ... (implementation remains the same) ...
        _f_name = self.__class__.__name__ + self.send_goto.__name__
        
        if not self._ensure_mode("GUIDED", _f_name, attempt_set_guided, item_timeout, retries):
            self.logger.error(f"{_f_name}: Vehicle must be in GUIDED mode.")
            return False
        
        if not self._ensure_armed(_f_name, attempt_arm, False, item_timeout, retries):
            self.logger.error(f"{_f_name}: Vehicle must be armed.")
            return False
        
        self.logger.info(f"{_f_name}: Pre-conditions met. Calling super().send_goto...")
        return super().send_goto(latitude, longitude, altitude_rel, speed, item_timeout, retries)

    # --- MODIFIED: Refined Guided Mode Control (Calls Base Method) ---
    def goto_local_ned(self,
                    north: Optional[float] = None, east: Optional[float] = None, down: Optional[float] = None,
                    vx: Optional[float] = None, vy: Optional[float] = None, vz: Optional[float] = None,
                    acc_x: Optional[float] = None, acc_y: Optional[float] = None, acc_z: Optional[float] = None,
                    yaw_deg: Optional[float] = None, yaw_rate_deg_s: Optional[float] = None,
                    use_body_frame_offset: bool = False,
                    type_mask_override: Optional[int] = None) -> bool:
        """
        Sends a SET_POSITION_TARGET_LOCAL_NED message to command the Copter
        in local NED coordinates. Allows specifying position, velocity, acceleration,
        yaw, and yaw rate targets.

        Performs Copter-specific pre-checks (GUIDED mode, armed) before calling
        the base handler's method to send the command.

        Args:
            north (Optional[float]): Target North position (m) in MAV_FRAME_LOCAL_NED/OFFSET_NED.
            east (Optional[float]): Target East position (m) in MAV_FRAME_LOCAL_NED/OFFSET_NED.
            down (Optional[float]): Target Down position (m) in MAV_FRAME_LOCAL_NED/OFFSET_NED (positive down).
            vx (Optional[float]): Target North velocity (m/s).
            vy (Optional[float]): Target East velocity (m/s).
            vz (Optional[float]): Target Down velocity (m/s) (positive down).
            acc_x (Optional[float]): Target North acceleration (m/s^2).
            acc_y (Optional[float]): Target East acceleration (m/s^2).
            acc_z (Optional[float]): Target Down acceleration (m/s^2) (positive down).
            yaw_deg (Optional[float]): Target yaw angle (degrees, 0=North, positive CW).
            yaw_rate_deg_s (Optional[float]): Target yaw rate (degrees/s, positive CW).
            use_body_frame_offset (bool): If True, use MAV_FRAME_LOCAL_OFFSET_NED (relative to current pos).
                                        If False (default), use MAV_FRAME_LOCAL_NED (origin is EKF origin).
            type_mask_override (Optional[int]): If provided, directly use this bitmask. Otherwise, it's
                                                calculated based on which arguments are not None.

        Returns:
            bool: True if the command was sent successfully (after pre-checks), False otherwise.
                Note: Success indicates the message was sent, not that the vehicle reached the target.
        """
        _f_name = self.__class__.__name__ + self.goto_local_ned.__name__

        # Copter-specific Pre-checks
        if not self._ensure_mode("GUIDED", _f_name, True, 1.0, 1):
             self.logger.error(f"{_f_name}: Vehicle must be in GUIDED mode.")
             return False
        if not self._ensure_armed(_f_name, False, False, 1.0, 1):
             self.logger.error(f"{_f_name}: Vehicle must be armed.")
             return False

        # --- Start: Logic moved from previous implementation to here (before calling base) ---
        # Determine coordinate frame
        if use_body_frame_offset:
            coordinate_frame = self.MAV_FRAME_LOCAL_OFFSET_NED
        else:
            coordinate_frame = self.MAV_FRAME_LOCAL_NED

        # Construct the type mask
        if type_mask_override is not None:
            type_mask = type_mask_override
            self.logger.debug(f"{_f_name}: Using provided type_mask override: {type_mask:#018b}")
        else:
            # Start with all fields ignored (all bits set)
            type_mask = 0b0000111111111111 # Ignore all pos, vel, acc, yaw, yaw_rate (Force is bit 9)

            # Un-ignore fields that are provided (clear the corresponding bit)
            if north is not None and east is not None and down is not None:
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_X_IGNORE
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_Y_IGNORE
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_Z_IGNORE
            
            if vx is not None and vy is not None and vz is not None:
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_VX_IGNORE
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_VY_IGNORE
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_VZ_IGNORE

            if acc_x is not None and acc_y is not None and acc_z is not None:
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_AX_IGNORE
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_AY_IGNORE
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_AZ_IGNORE

            # Prioritize yaw rate if both yaw and yaw rate are provided
            if yaw_rate_deg_s is not None:
                type_mask &= ~self.POSITION_TARGET_TYPEMASK_YAW_RATE_IGNORE
                if yaw_deg is not None:
                    self.logger.warning(f"{_f_name}: Both yaw_deg and yaw_rate_deg_s provided. Yaw Rate takes precedence.")
            #elif yaw_deg is not None:
                #type_mask &= ~self.POSITION_TARGET_TYPEMASK_YAW_ANGLE_IGNORE
            
            # Ensure Force is ignored
            type_mask |= self.POSITION_TARGET_TYPEMASK_FORCE_SET
            
            self.logger.debug(f"{_f_name}: Calculated type_mask: {type_mask:#018b}")

        # Check if anything is being commanded
        ignore_all_mask = (self.POSITION_TARGET_TYPEMASK_X_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_Y_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_Z_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_VX_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_VY_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_VZ_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_AX_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_AY_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_AZ_IGNORE |
                            self.POSITION_TARGET_TYPEMASK_FORCE_SET 
                            #self.POSITION_TARGET_TYPEMASK_YAW_ANGLE_IGNORE |
                            #self.POSITION_TARGET_TYPEMASK_YAW_RATE_IGNORE
                            )
        if type_mask == ignore_all_mask:
            self.logger.warning(f"{_f_name}: No target fields provided (all ignored in mask). Command may have no effect.")
            return True

        # Prepare parameters (convert angles, default Nones)
        yaw_rad = math.radians(yaw_deg) if yaw_deg is not None else 0.0
        yaw_rate_rad_s = math.radians(yaw_rate_deg_s) if yaw_rate_deg_s is not None else 0.0
        pos_n = float(north) if north is not None else 0.0
        pos_e = float(east) if east is not None else 0.0
        pos_d = float(down) if down is not None else 0.0
        vel_n = float(vx) if vx is not None else 0.0
        vel_e = float(vy) if vy is not None else 0.0
        vel_d = float(vz) if vz is not None else 0.0
        acc_n = float(acc_x) if acc_x is not None else 0.0
        acc_e = float(acc_y) if acc_y is not None else 0.0
        acc_d = float(acc_z) if acc_z is not None else 0.0
        # --- End: Logic moved ---

        # Call the base handler's method to actually send the command
        # Pass all calculated/prepared values
        return super()._send_position_target_local_ned(
            coordinate_frame=coordinate_frame,
            type_mask=type_mask,
            x=pos_n, y=pos_e, z=pos_d,
            vx=vel_n, vy=vel_e, vz=vel_d,
            afx=acc_n, afy=acc_e, afz=acc_d,
            yaw=yaw_rad, yaw_rate=yaw_rate_rad_s
        )

    # --- Overridden State Getters ---
    def get_flight_mode(self) -> Optional[str]:
        # ... (implementation remains the same) ...
        with self._state_lock:
            if self._base_mode is None or self._custom_mode is None:
                return None
            
            if bool(self._base_mode & self.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED):
                mode_name = self.COPTER_MODE_NAMES.get(self._custom_mode)
                if mode_name: 
                    return mode_name
                
                else:
                    self.logger.warning(f"Received unknown Copter custom mode: {self._custom_mode}")
                    return f"CUSTOM({self._custom_mode})"
            
            else:
                return super().get_flight_mode() 

    # --- Copter-Specific State Getters ---
    def is_landed(self) -> Optional[bool]:
        # ... (implementation remains the same) ...
        landed_state_id = self.get_landed_state_id() 
        
        if landed_state_id is None:
            return None
        
        if landed_state_id == mavutil.mavlink.MAV_LANDED_STATE_ON_GROUND:
            return True
        
        else:
            return False

