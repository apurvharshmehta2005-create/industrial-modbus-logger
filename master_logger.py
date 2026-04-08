
# STEP 1: IMPORT REQUIRED LIBRARIES
import socket                  
import struct                  
import time                    
import csv                     
import os                      
from datetime import datetime 

# STEP 2: NETWORK & FOLDER SETTINGS 
TCP_PORT = 20108               

# NEW SAVE LOCATION DIRECTLY IN FACTORY_CODE FOLDER 
MAIN_SAVE_FOLDER = r"C:\Users\Apurv\Desktop\Factory_Code\Data_Logs"

# THE FACTORY ZONES (MULTI-GATEWAY SYSTEM) 
GENERATING_UNITS = {
    "Unit_1": {"ids": list(range(1, 26)),   "ip": '192.168.1.101'},
    "Unit_2": {"ids": list(range(26, 67)),  "ip": '192.168.1.102'},
    "Unit_3": {"ids": list(range(67, 108)), "ip": '192.168.1.103'} 
}

# THE CLEAN ROW-BY-ROW HEADERS (NOT TRANSPOSED)
CSV_HEADERS = [
    'Date', 'Time', 'Slave ID', 'Meter Brand', 'Energy (kWh)', 'Power (kW)', 
    'Avg VLL (V)', 'Avg VLN (V)', 'Avg Current (A)', 
    'Phase R Current (IR)', 'Phase Y Current (Iy)', 'Phase B Current (Ib)'
]
DAILY_HEADERS = ['Date', 'Time', 'Slave ID', 'Energy (kWh)', 'Daily energy consumption']

# STEP 3: METER RULEBOOKS (SWAPPED) 
TRINITY_REGISTERS = {
    'Energy (kWh)': (3029, 100.0, 3, 'long'), 'Power (kW)': (3001, 100.0, 3, 'long'),
    'Avg VLL (V)': (3007, 100.0, 3, 'long'), 'Avg VLN (V)': (3009, 100.0, 3, 'long'), 
    'Avg Current (A)': (3011, 100.0, 3, 'long'), 'Phase R Current (IR)': (3041, 100.0, 3, 'long'),
    'Phase Y Current (Iy)': (3071, 100.0, 3, 'long'), 'Phase B Current (Ib)': (3101, 100.0, 3, 'long')
}

SCHNEIDER_REGISTERS = {
    'Energy (kWh)': (3030, 100.0, 3, 'long'), 'Power (kW)': (3002, 100.0, 3, 'long'),
    'Avg VLL (V)': (3024, 100.0, 3, 'long'), 'Avg VLN (V)': (3018, 100.0, 3, 'long'), 
    'Avg Current (A)': (3012, 100.0, 3, 'long'), 'Phase R Current (IR)': (3014, 100.0, 3, 'long'),
    'Phase Y Current (Iy)': (3016, 100.0, 3, 'long'), 'Phase B Current (Ib)': (3020, 100.0, 3, 'long') 
}

RISHABH_REGISTERS = {
    'Energy (kWh)': (84, 1.0, 4, 'float'), 'Power (kW)': (42, 1.0, 4, 'float'),
    'Avg VLL (V)': (14, 1.0, 4, 'float'), 'Avg VLN (V)': (6, 1.0, 4, 'float'),  
    'Avg Current (A)': (22, 1.0, 4, 'float'), 'Phase R Current (IR)': (16, 1.0, 4, 'float'),
    'Phase Y Current (Iy)': (18, 1.0, 4, 'float'), 'Phase B Current (Ib)': (20, 1.0, 4, 'float')
}
# STEP 4: PREPARE BASE FOLDERS & MEMORY 
print(f"Setting up base folder: {MAIN_SAVE_FOLDER}")
if not os.path.exists(MAIN_SAVE_FOLDER):
    os.makedirs(MAIN_SAVE_FOLDER)

meter_brand_memory = {} 
daily_kwh_memory = {}   
offline_memory = {} 

for unit_name in GENERATING_UNITS.keys():
    # Creates Unit_1, Unit_2, Unit_3 folders directly under Data_Logs
    unit_path = os.path.join(MAIN_SAVE_FOLDER, unit_name)
    if not os.path.exists(unit_path):
        os.makedirs(unit_path) 

    main_file = os.path.join(unit_path, f"updated3M_{unit_name}.csv")
    daily_file = os.path.join(unit_path, f"daily_KWH_{unit_name}.csv")
    
    if not os.path.isfile(main_file):
        with open(main_file, mode='w', newline='') as f:
            csv.writer(f).writerow(CSV_HEADERS)
            
    if not os.path.isfile(daily_file):
        with open(daily_file, mode='w', newline='') as df:
            csv.writer(df).writerow(DAILY_HEADERS)
    else:
        # Standard Memory Recall Engine (Adapted back from Monthly format)
        try:
            with open(daily_file, mode='r') as df:
                reader = csv.reader(df)
                next(reader, None) 
                for row in reader:
                    if len(row) >= 4:
                        try:
                            saved_date = row[0]
                            saved_id = int(row[2])
                            saved_kwh = float(row[3])
                            daily_kwh_memory[saved_id] = {"date": saved_date, "kwh": saved_kwh}
                        except ValueError:
                            pass 
        except Exception as e:
            print(f"Notice: Could not load history for {unit_name}: {e}")

# STEP 5: PURE ETHERNET TCP ENGINE 
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
    else:
        return struct.unpack('>I', data_bytes)[0]

def detect_meter_type(sock, sid):
    try:
        if (read_raw_tcp(sock, sid, 3009, 3, 'long', 0.5) / 100.0) > 50.0: return "TRINITY"
    except: pass
    try:
        if (read_raw_tcp(sock, sid, 3018, 3, 'long', 0.5) / 100.0) > 50.0: return "SCHNEIDER"
    except: pass
    try:
        if read_raw_tcp(sock, sid, 6, 4, 'float', 0.5) > 50.0: return "RISHABH"
    except: pass
    return None

# SMART SOCKET MANAGER 
active_sockets = {}

def connect_to_usr(target_ip):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.5) 
    s.connect((target_ip, TCP_PORT))
    return s

def get_active_socket(target_ip):
    if target_ip not in active_sockets:
        active_sockets[target_ip] = connect_to_usr(target_ip)
    return active_sockets[target_ip]

# STEP 6: THE INFINITE LOGGING LOOP 
# Removed "Transposed Monthly Scanner" print
print(f"\n--- System Armed! Initializing Classic Row-Based Scanner ---")

while True:
    current_time_obj = datetime.now()
    current_date_str = current_time_obj.strftime('%Y-%m-%d')
    current_time_str = current_time_obj.strftime('%H:%M:%S')
    
    for unit_name, unit_info in GENERATING_UNITS.items():
        slave_ids = unit_info["ids"]
        current_ip = unit_info["ip"]
        
        if len(slave_ids) == 0: continue 
        
        # New clean file paths
        unit_path = os.path.join(MAIN_SAVE_FOLDER, unit_name)
        main_file = os.path.join(unit_path, f"updated3M_{unit_name}.csv")
        daily_file = os.path.join(unit_path, f"daily_KWH_{unit_name}.csv")
        
        unit_data_buffer = []
        unit_daily_buffer = []

        print(f"\n[{current_date_str} {current_time_str}] Scanning {unit_name} via IP {current_ip}...")

        try:
            usr_socket = get_active_socket(current_ip)
        except Exception as e:
            print(f"  [NETWORK ERROR] Could not reach Gateway at {current_ip}. Skipping {unit_name}...")
            continue

        for sid in slave_ids:
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
                        
                if current_brand == "SCHNEIDER": active_registers = SCHNEIDER_REGISTERS
                elif current_brand == "TRINITY": active_registers = TRINITY_REGISTERS
                elif current_brand == "RISHABH": active_registers = RISHABH_REGISTERS
                
                # REVERTED TO CLASSIC ROW-DATA BUILDING
                row_data = [current_date_str, current_time_str, sid, current_brand]
                
                for name, (addr, divisor, fcode, dtype) in active_registers.items():
                    val = None
                    for _ in range(3): 
                        try:
                            val = read_raw_tcp(usr_socket, sid, addr, fcode, dtype, 1.5)
                            break 
                        except: 
                            time.sleep(0.1) 
                    
                    if val is None:
                        raise ValueError("Meter stopped responding mid-scan")
                        
                    row_data.append(val / divisor)
                
                # Append online meter to main buffer
                unit_data_buffer.append(row_data)
                print(f"     [DATA SAVED] Reading complete.")
                
                if current_time_obj.hour >= 8:
                    energy_kwh = row_data[4] # Energy (kWh) is at index 4 in row_data
                    
                    if sid not in daily_kwh_memory:
                        daily_kwh_memory[sid] = {"date": None, "kwh": energy_kwh}
                    
                    if daily_kwh_memory[sid]["date"] != current_date_str:
                        prev_kwh = daily_kwh_memory[sid]["kwh"]
                        consumption = round(energy_kwh - prev_kwh, 2) if daily_kwh_memory[sid]["date"] is not None else 0.0
                        
                        unit_daily_buffer.append([current_date_str, current_time_str, sid, energy_kwh, consumption])
                        
                        daily_kwh_memory[sid]["date"] = current_date_str
                        daily_kwh_memory[sid]["kwh"] = energy_kwh

            except Exception:
                print(f"     [ERROR] Connection lost on ID {sid}. Blacklisting for 60 seconds.")
                offline_memory[sid] = time.time()

        # CLASSIC BUFFER SAVING ENGINE (NOT PIVOTED) 
        try:
            if len(unit_data_buffer) > 0:
                with open(main_file, mode='a', newline='') as f:
                    # Append all online meters as new rows
                    csv.writer(f).writerows(unit_data_buffer) 
                print(f"  -> Successfully wrote {len(unit_data_buffer)} rows to CSV.")
            
            if len(unit_daily_buffer) > 0:
                with open(daily_file, mode='a', newline='') as df:
                    csv.writer(df).writerows(unit_daily_buffer)

        except PermissionError:
            print(f"⚠️ A CSV file in {unit_name} is open in Excel. Close it to resume saving.")
        except Exception as e:
            try:
                active_sockets[current_ip].close()
                del active_sockets[current_ip]
            except: pass
