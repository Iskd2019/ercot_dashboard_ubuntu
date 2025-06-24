import schedule
import time
import subprocess

def run_script():
    print("Running scheduled task...")
    subprocess.run([
        #python程序的位置
        r"C:\Users\usr_p013\anaconda3\python.exe",
        # get_json_and_push_to_postgreDB.py的位置
        r"C:\Users\usr_p013\Desktop\get_json_from_7_days_data_and_push_to_Grafana\get_json_and_push_to_postgreDB.py"
    ])
    print("Finished execution.")

# Schedule every 5 minutes
schedule.every(2).minutes.do(run_script)

print("Scheduler started.")
while True:
    schedule.run_pending()
    time.sleep(1)