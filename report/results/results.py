import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ---------------------------
# Lettura dei dati da CSV
# ---------------------------
# Algoritmo 1: ogni file contiene una colonna con un numero per riga
alg1_v1 = pd.read_csv('j1-no-opt.csv', header=None)
alg1_v2 = pd.read_csv('j1-opt.csv', header=None)

# Algoritmo 2: ogni file contiene 5 colonne per riga (si plottarà la quinta colonna)
alg2_v1 = pd.read_csv('j2-no-opt.csv', header=None)
alg2_v2 = pd.read_csv('j2-opt.csv', header=None)

# ---------------------------
# Creazione dei grafici
# ---------------------------
# Creiamo una figura con due sottografici (uno per ciascun algoritmo)
fig, axs = plt.subplots(2, 1, figsize=(10, 8))

# 1. Grafico a barre per Algoritmo 1
n = len(alg1_v1)  # numero di campioni
indices = np.arange(n)
bar_width = 0.35

# Plottiamo i valori di ciascuna versione affiancati
axs[0].bar(indices - bar_width/2, alg1_v1[0], width=bar_width, label='Non optimized version')
axs[0].bar(indices + bar_width/2, alg1_v2[0], width=bar_width, label='Optimized version')
axs[0].set_title('Job 1: Average Song per artist')

axs[0].legend()
axs[0].grid(True)

# 2. Grafico a barre per Algoritmo 2 (plottando la quinta colonna)
n2 = len(alg2_v1)  # numero di campioni
indices2 = np.arange(n2)

# Si assume che la quinta colonna sia quella di interesse (indice 4)
axs[1].bar(indices2 - bar_width/2, alg2_v1[4], width=bar_width, label='Non optimized version')
axs[1].bar(indices2 + bar_width/2, alg2_v2[4], width=bar_width, label='Optimized version')
axs[1].set_title(f'Job 2: Most popular song - {alg2_v1[1].iloc[0]}')
axs[1].set_ylabel('Shared playlists')
axs[1].legend()
axs[1].grid(True)

plt.tight_layout()
plt.show()
