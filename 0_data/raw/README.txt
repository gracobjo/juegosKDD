Coloca aquí los CSV del recomendador (nombres exactos):

  steam_games_behaviors.csv  ← Kaggle tamber/steam-video-games (archivo steam-200k.csv renombrado)
  steam_reviews.csv          ← Kaggle andrewmvd/steam-reviews (el script normaliza columnas al loader Hive)

Desde la raíz del repo (carpeta juegosKDD):

  Opción 1 — ~/.kaggle/kaggle.json (token Kaggle en tu home)

  Opción 2 — En .env (kagglehub recomienda token):
    KAGGLE_API_TOKEN=...   (Settings → API en Kaggle)
    # o bien KAGGLE_USERNAME + KAGGLE_KEY (se puede crear ~/.kaggle/kaggle.json)

  Luego (venv activado):
  python3 0_data/kaggle/download_datasets.py

  Solo comprobar ruta de descarga con kagglehub (no es bash; hay que invocar Python):
  python3 -c "import kagglehub; print(kagglehub.dataset_download('andrewmvd/steam-reviews'))"

  Si sale «No module named 'kagglehub'»: el venv no tiene el paquete (p. ej. venv creado antes
  de añadirlo a requirements.txt). Instálalo:
    pip install "kagglehub>=0.2.0"
  o reinstala todo:
    pip install -r requirements.txt
  El script download_datasets.py también intenta instalar kagglehub automáticamente si falta.
