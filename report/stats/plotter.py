import json
import matplotlib.pyplot as plt
from collections import defaultdict
import glob

# Percorso ai file (esempio: *.txt per raccogliere tutti i file di log nella directory corrente)
file_paths = glob.glob("history/*")

# Risultati aggregati
results = defaultdict(lambda: {"total_job_time": 0, "job_count": 0, "shuffled_data_mb": 0, "task_count": 0})

# Analizza ciascun file
for file_path in file_paths:
    with open(file_path, "r") as file:
        logs = [json.loads(line) for line in file]
    
    # Filtra eventi utili
    job_starts = {log["Job ID"]: log["Submission Time"] for log in logs if log["Event"] == "SparkListenerJobStart"}
    job_ends = {log["Job ID"]: log["Completion Time"] for log in logs if log["Event"] == "SparkListenerJobEnd"}
    tasks = [log for log in logs if log["Event"] == "SparkListenerTaskEnd"]
    
    # Calcola il tempo totale dei job
    total_job_time = sum(job_ends[job] - job_starts[job] for job in job_starts if job in job_ends)
    job_count = len(job_starts)
    
    # Calcola i dati shufflati (in MB)
    shuffled_data_mb = sum(task["Task Metrics"]["Shuffle Write Metrics"]["Shuffle Bytes Written"] / (1024 * 1024)
                           for task in tasks if "Task Metrics" in task and "Shuffle Write Metrics" in task["Task Metrics"])
    
    # Conta il numero totale di task
    task_count = len(tasks)
    
    # Aggiorna i risultati
    results[file_path]["total_job_time"] = total_job_time
    results[file_path]["job_count"] = job_count
    results[file_path]["shuffled_data_mb"] = shuffled_data_mb
    results[file_path]["task_count"] = task_count

# Prepara i dati per il grafico
file_names = list(results.keys())
job_times = [results[file]["total_job_time"] for file in file_names]
job_counts = [results[file]["job_count"] for file in file_names]
shuffled_data = [results[file]["shuffled_data_mb"] for file in file_names]
task_counts = [results[file]["task_count"] for file in file_names]

# Grafico
plt.figure(figsize=(15, 10))

# Tempo totale dei job
plt.subplot(2, 2, 1)
plt.bar(file_names, job_times, color="skyblue")
plt.title("Tempo totale dei Job", fontsize=14)
plt.ylabel("Tempo totale (ms)", fontsize=12)
plt.xticks(rotation=45, ha="right")

# Numero di job completati
plt.subplot(2, 2, 2)
plt.bar(file_names, job_counts, color="orange")
plt.title("Numero di Job Completati", fontsize=14)
plt.ylabel("Numero di Job", fontsize=12)
plt.xticks(rotation=45, ha="right")

# Dati shufflati
plt.subplot(2, 2, 3)
plt.bar(file_names, shuffled_data, color="green")
plt.title("Dati Shufflati", fontsize=14)
plt.ylabel("Dati Shufflati (MB)", fontsize=12)
plt.xticks(rotation=45, ha="right")

# Numero di task
plt.subplot(2, 2, 4)
plt.bar(file_names, task_counts, color="purple")
plt.title("Numero di Task Completati", fontsize=14)
plt.ylabel("Numero di Task", fontsize=12)
plt.xticks(rotation=45, ha="right")

plt.tight_layout()
plt.show()
