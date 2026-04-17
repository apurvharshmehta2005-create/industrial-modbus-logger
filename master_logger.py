# ==========================================
# --- STEP 1: IMPORT REQUIRED LIBRARIES ---
# ==========================================
import socket                  
import struct                  
import time                    
import csv                     
import os                      
from datetime import datetime  

# ==========================================
# --- STEP 2: NETWORK & FOLDER SETTINGS ---
# ==========================================
TCP_PORT = 20108               

MAIN_SAVE_FOLDER = r"C:\Users\Apurv\Desktop\Factory_Code\Data_Logs"

GENERATING_UNITS = {
    "Unit_1": {"ids": list(range(1, 26)),   "ip": '192.168.25.201'},
    "Unit_2": {"ids": list(range(26, 67)),  "ip": '192.168.25.202'},
    "Unit_3": {"ids": list(range(67, 108)), "ip": '192.168.25.203'},
    "VG":     {"ids": list(range(1, 16)),   "ip": '192.168.25.204'} 
}

# The Custom Name Dictionary for the VG Gateway
VG_NAMES = {
    1: "ID 1 (MAIN)",
    2: "ID 2 (DRIVE)",
    3: "ID 3 (HEATING)",
    4: "ID 4 (UTILITY 1)",
    5: "ID 5 (UTILITY 2)",
    6: "ID 6 (STP PLANT)",
    7: "ID 7 (COMPRESSOR)",
    8: "ID 8 (AREA LIGHTING)",
    9: "ID 9 (LIGHTING)",
    10: "ID 10 (UPS)",
    11: "ID 11 (APFC PANEL)"
}

DAILY_KWH_LIMIT = 500.0  

DATA_PARAMETERS = [
    'Energy (kWh)', 'Energy (MWh)', 'Power (kW)', 
    'Avg VLL (V)', 'Avg VLN (V)', 'Avg Current (A)', 
    'Phase R Current (IR)', 'Phase Y Current (Iy)', 'Phase B Current (Ib)'
]
DAILY_PARAMETERS = [
    'Energy (kWh)', 'Daily energy consumption', 
    'Target Limit (kWh)', 'SCADA Status'
]

# ==========================================
# --- STEP 3: THE VERIFIED RULEBOOKS ---
# ==========================================

# Map 1: EM6436 (Working perfectly for IDs 2-15)
SCHNEIDER_6436_REGISTERS = {
    'Energy (kWh)': (3960, 1000.0, 3, 'float_cdab'), 
    'Energy (MWh)': (3960, 1000000.0, 3, 'float_cdab'), 
    'Power (kW)': (3902, 1000.0, 3, 'float_cdab'),  
    'Avg VLL (V)': (3908, 1.0, 3, 'float_cdab'), 
    'Avg VLN (V)': (3910, 1.0, 3, 'float_cdab'), 
    'Avg Current (A)': (3912, 1.0, 3, 'float_cdab'), 
    'Phase R Current (IR)': (3928, 1.0, 3, 'float_cdab'),
    'Phase Y Current (Iy)': (3942, 1.0, 3, 'float_cdab'), 
    'Phase B Current (Ib)': (3956, 1.0, 3, 'float_cdab') 
}

# Map 2: EM6400NG (The 100% Verified Map for ID 1)
SCHNEIDER_6400_REGISTERS = {
    'Energy (kWh)': (2699, 1.0, 3, 'float'), 
    'Energy (MWh)': (2699, 1000.0, 3, 'float'), 
    'Power (kW)': (3059, 1.0, 3, 'float'),   
    'Avg VLL (V)': (3019, 1.0, 3, 'float'), 
    'Avg VLN (V)': (3027, 1.0, 3, 'float'), 
    'Avg Current (A)': (3009, 1.0, 3, 'float'), 
    'Phase R Current (IR)': (2999, 1.0, 3, 'float'),
    'Phase Y Current (Iy)': (3001, 1.0, 3, 'float'), 
    'Phase B Current (Ib)': (3003, 1.0, 3, 'float') 
}

TRINITY_REGISTERS = {
    'Energy (kWh)': (3029, 100.0, 3, 'long'), 'Energy (MWh)': (3029, 100000.0, 3, 'long'), 
    'Power (kW)': (3001, 100.0, 3, 'long'), 'Avg VLL (V)': (3007, 100.0, 3, 'long'), 
    'Avg VLN (V)': (3009, 100.0, 3, 'long'), 'Avg Current (A)': (3011, 100.0, 3, 'long'), 
    'Phase R Current (IR)': (3041, 100.0, 3, 'long'), 'Phase Y Current (Iy)': (3071, 100.0, 3, 'long'), 
    'Phase B Current (Ib)': (3101, 100.0, 3, 'long')
}

RISHABH_REGISTERS = {
    'Energy (kWh)': (84, 1.0, 4, 'float'), 'Energy (MWh)': (84, 1000.0, 4, 'float'), 
    'Power (kW)': (42, 1.0, 4, 'float'), 'Avg VLL (V)': (14, 1.0, 4, 'float'), 
    'Avg VLN (V)': (6, 1.0, 4, 'float'), 'Avg Current (A)': (22, 1.0, 4, 'float'), 
    'Phase R Current (IR)': (16, 1.0, 4, 'float'), 'Phase Y Current (Iy)': (18, 1.0, 4, 'float'), 
    'Phase B Current (Ib)': (20, 1.0, 4, 'float')
}

# ==========================================
# --- STEP 4: PREPARE FOLDERS & MEMORY ---
# ==========================================
print(f"Setting up base folder: {MAIN_SAVE_FOLDER}")
if not os.path.exists(MAIN_SAVE_FOLDER):
    os.makedirs(MAIN_SAVE_FOLDER)

meter_brand_memory = {} 
daily_kwh_memory = {}   
offline_memory = {} 

current_startup_month = datetime.now().strftime('%B_%Y')

for unit_name, unit_info in GENERATING_UNITS.items():
    unit_month_path = os.path.join(MAIN_SAVE_FOLDER, unit_name, current_startup_month)
    daily_file = os.path.join(unit_month_path, f"daily_KWH_{unit_name}_{current_startup_month}.csv")
    
    if os.path.isfile(daily_file):
        try:
            with open(daily_file, mode='r', encoding='utf-8-sig') as df:
                reader = csv.reader(df)
                for row in reader:
                    if len(row) > 3 and row[2] == 'Energy (kWh)':
                        pass 
        except: pass

# ==========================================
# --- STEP 5: PURE ETHERNET TCP ENGINE ---
# ==========================================
def crc16(data: bytes):
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for _ in range(8):
            if (crc & 1) != 0:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    return struct.pack('<H', crc)

def read_raw_tcp(sock, slave_id, address, fcode, dtype, custom_timeout=1.5):
    req = struct.pack('>BBHH', slave_id, fcode, address, 2)
    req += crc16(req)
    
    sock.setblocking(False)
    try:
        while sock.recv(1024): pass 
    except: pass
    
    sock.settimeout(custom_timeout)
    sock.sendall(req) 
    
    resp = b''
    start_time = time.time()
    while len(resp) < 9: 
        if (time.time() - start_time) > custom_timeout: 
            raise TimeoutError("Network Lag Timeout")
        try:
            chunk = sock.recv(9 - len(resp))
            if not chunk: break
            resp += chunk
        except socket.error:
            continue

    if len(resp) < 9 or resp[0] != slave_id or resp[1] != fcode:
        raise ValueError("Corrupted Data Packet")
    if crc16(resp[:-2]) != resp[-2:]:
        raise ValueError("CRC Signature Failed")

    data_bytes = resp[3:7] 
    
    if dtype == 'float':
        return struct.unpack('>f', data_bytes)[0]
    elif dtype == 'float_cdab':
        swapped_bytes = data_bytes[2:4] + data_bytes[0:2] 
        return struct.unpack('>f', swapped_bytes)[0]
    else:
        return struct.unpack('>I', data_bytes)[0]

def detect_meter_type(sock, sid):
    try:
        # Pings EM6436 meters
        val = read_raw_tcp(sock, sid, 3908, 3, 'float_cdab', 0.6) 
        if 100.0 <= val <= 10000.0: return "SCHNEIDER_6436"
    except: pass

    try:
        # Pings ID 1 using the exact address we proved works!
        val = read_raw_tcp(sock, sid, 3019, 3, 'float', 0.6) 
        if 100.0 <= val <= 10000.0: return "SCHNEIDER_6400"
    except: pass
    
    try:
        val = read_raw_tcp(sock, sid, 3007, 3, 'long', 0.6)
        if 100.0 <= (val / 100.0) <= 10000.0: return "TRINITY"
    except: pass
    
    try:
        val = read_raw_tcp(sock, sid, 14, 4, 'float', 0.6)
        if 100.0 <= val <= 10000.0: return "RISHABH"
    except: pass
    
    return None

active_sockets = {}

def get_active_socket(target_ip):
    if target_ip not in active_sockets:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.5) 
        s.connect((target_ip, TCP_PORT))
        active_sockets[target_ip] = s
    return active_sockets[target_ip]

# ==========================================
# --- STEP 6: THE INFINITE LOGGING LOOP ---
# ==========================================
print(f"\n--- System Armed! Initializing DYNAMIC Transposed Monthly Scanner ---")

while True: 
    current_time_obj = datetime.now()
    current_date_str = current_time_obj.strftime('%Y-%m-%d')
    current_time_str = current_time_obj.strftime('%H:%M:%S')
    current_month_str = current_time_obj.strftime('%B_%Y') 
    
    for unit_name, unit_info in GENERATING_UNITS.items():
        slave_ids = unit_info["ids"]
        current_ip = unit_info["ip"]
        
        if len(slave_ids) == 0: continue 
        
        unit_month_path = os.path.join(MAIN_SAVE_FOLDER, unit_name, current_month_str)
        if not os.path.exists(unit_month_path):
            os.makedirs(unit_month_path)

        main_file = os.path.join(unit_month_path, f"updated3M_{unit_name}_{current_month_str}.csv")
        daily_file = os.path.join(unit_month_path, f"daily_KWH_{unit_name}_{current_month_str}.csv")

        print(f"\n[{current_date_str} {current_time_str}] Scanning {unit_name} via IP {current_ip}...")

        try:
            usr_socket = get_active_socket(current_ip)
        except Exception as e:
            print(f"  [NETWORK ERROR] Could not reach Gateway at {current_ip}. Skipping...")
            continue

        cycle_data = {} 
        daily_cycle_data = {}
        online_sids = [] 
        daily_triggered = False

        for sid in slave_ids:
            time.sleep(0.2) 
            
            if sid in offline_memory:
                if (time.time() - offline_memory[sid]) < 60.0:
                    continue 

            print(f"  -> Pinging ID {sid}...") 
            try:
                current_brand = meter_brand_memory.get(sid)
                
                if current_brand is None:
                    current_brand = detect_meter_type(usr_socket, sid)
                    if current_brand:
                        meter_brand_memory[sid] = current_brand
                        if sid in offline_memory: del offline_memory[sid] 
                        print(f"     [SUCCESS] Discovered {current_brand}!")
                    else:
                        print(f"     [FAILED] Offline. Blacklisting ID {sid} for 60 seconds.")
                        offline_memory[sid] = time.time() 
                        continue 
                        
                if current_brand == "SCHNEIDER_6436": active_registers = SCHNEIDER_6436_REGISTERS
                elif current_brand == "SCHNEIDER_6400": active_registers = SCHNEIDER_6400_REGISTERS
                elif current_brand == "TRINITY": active_registers = TRINITY_REGISTERS
                elif current_brand == "RISHABH": active_registers = RISHABH_REGISTERS
                
                temp_meter_data = {}
                
                for param_name, (addr, divisor, fcode, dtype) in active_registers.items():
                    val = None
                    for _ in range(3): 
                        try:
                            val = read_raw_tcp(usr_socket, sid, addr, fcode, dtype, 1.5)
                            break 
                        except: 
                            time.sleep(0.1) 
                    
                    if val is None:
                        raise ValueError("Meter stopped responding mid-scan")
                        
                    temp_meter_data[param_name] = val / divisor 
                
                online_sids.append(sid)
                cycle_data[sid] = temp_meter_data
                print(f"     [DATA SAVED] Reading complete.")
                
                if current_time_obj.hour >= 8:
                    energy_kwh = temp_meter_data['Energy (kWh)']
                    if sid not in daily_kwh_memory:
                        daily_kwh_memory[sid] = {"date": None, "kwh": energy_kwh}
                    
                    if daily_kwh_memory[sid]["date"] != current_date_str:
                        daily_triggered = True 
                        prev_kwh = daily_kwh_memory[sid]["kwh"]
                        consumption = round(energy_kwh - prev_kwh, 2) if daily_kwh_memory[sid]["date"] is not None else 0.0
                        
                        status = "EXCEEDED LIMIT" if consumption > DAILY_KWH_LIMIT else "NORMAL"
                        
                        daily_cycle_data[sid] = {
                            'Energy (kWh)': energy_kwh, 
                            'Daily energy consumption': consumption,
                            'Target Limit (kWh)': DAILY_KWH_LIMIT,
                            'SCADA Status': status
                        }
                        
                        daily_kwh_memory[sid]["date"] = current_date_str
                        daily_kwh_memory[sid]["kwh"] = energy_kwh

            except Exception as e:
                print(f"     [ERROR] Connection lost on ID {sid}: {e}. Blacklisting for 60 seconds.")
                offline_memory[sid] = time.time()

        try:
            if len(online_sids) > 0:
                with open(main_file, mode='a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    
                    # Custom Header Logic
                    block_headers = ['Date', 'Time', 'Parameter']
                    for sid in online_sids:
                        if unit_name == "VG" and sid in VG_NAMES:
                            block_headers.append(VG_NAMES[sid])
                        else:
                            block_headers.append(f"ID {sid}")
                            
                    writer.writerow(block_headers)
                
                    for param in DATA_PARAMETERS:
                        row = [current_date_str, current_time_str, param]
                        for sid in online_sids:
                            val = cycle_data[sid].get(param, "")
                            if isinstance(val, float): val = f"{val:.2f}" 
                            row.append(val)
                        writer.writerow(row)
                    
                    writer.writerow([]) 
                    
                print(f"  -> Successfully pivoted {len(online_sids)} online meters to folder.")

            if daily_triggered and len(daily_cycle_data) > 0:
                with open(daily_file, mode='a', newline='', encoding='utf-8-sig') as df:
                    d_writer = csv.writer(df)
                    
                    daily_online_sids = list(daily_cycle_data.keys())
                    d_headers = ['Date', 'Time', 'Parameter']
                    for sid in daily_online_sids:
                        if unit_name == "VG" and sid in VG_NAMES:
                            d_headers.append(VG_NAMES[sid])
                        else:
                            d_headers.append(f"ID {sid}")
                            
                    d_writer.writerow(d_headers)
                        
                    for param in DAILY_PARAMETERS:
                        d_row = [current_date_str, current_time_str, param]
                        for sid in daily_online_sids:
                            d_row.append(daily_cycle_data[sid].get(param, ""))
                        d_writer.writerow(d_row)
                    
                    d_writer.writerow([])
                        
        except Exception as e:
            print(f"  [FILE ERROR] Failed to save data to CSV: {e}") 
            try:
                active_sockets[current_ip].close()
                del active_sockets[current_ip]
            except: pass
