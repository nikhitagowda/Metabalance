# # import os
# # import io
# # import csv
# # import json
# # import time
# # import math
# # from datetime import datetime
# # from typing import Dict, Any, List

# # from flask import Flask, request, jsonify

# # # Google cloud clients (some may be optional)
# # from google.cloud import storage, firestore  # storage and firestore expected
# # # secretmanager might not be installed in some environments; import defensively
# # try:
# #     from google.cloud import secretmanager
# # except Exception:
# #     secretmanager = None  # optional

# # # --- Configuration from environment ---
# # PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GOOGLE_PROJECT") or "metabalance-ai"
# # GCS_BUCKET = os.environ.get("GCS_BUCKET", "")  # set this in Cloud Run env
# # FIRESTORE_ENABLED = os.environ.get("FIRESTORE_ENABLED", "false").lower() in ("1", "true", "yes")
# # PORT = int(os.environ.get("PORT", 8080))

# # # Init Flask
# # app = Flask(__name__)

# # # Initialize cloud clients lazily (so import errors don't crash)
# # storage_client = None
# # firestore_client = None

# # def get_storage_client():
# #     global storage_client
# #     if storage_client is None:
# #         storage_client = storage.Client()
# #     return storage_client

# # def get_firestore_client():
# #     global firestore_client
# #     if firestore_client is None:
# #         firestore_client = firestore.Client()
# #     return firestore_client

# # # --- Utility: read CSV from GCS path like "users/abc.csv" or "demo_user/xxx.csv" ---
# # def read_csv_from_gcs(gcs_path: str) -> List[Dict[str, str]]:
# #     """
# #     gcs_path: path inside bucket (no "gs://bucket/")
# #     returns list of dict rows (keys from header)
# #     """
# #     if not GCS_BUCKET:
# #         raise RuntimeError("GCS_BUCKET not configured")
# #     client = get_storage_client()
# #     bucket = client.bucket(GCS_BUCKET)
# #     blob = bucket.blob(gcs_path)
# #     if not blob.exists():
# #         raise FileNotFoundError(f"No such object: {GCS_BUCKET}/{gcs_path}")
# #     data = blob.download_as_text(encoding="utf-8")
# #     reader = csv.DictReader(io.StringIO(data))
# #     return list(reader)

# # # --- CSV uploader endpoint (multipart) - keeps compatibility with your local flow ---
# # @app.route("/api/upload", methods=["POST"])
# # def api_upload():
# #     file = request.files.get("file")
# #     if not file:
# #         return jsonify({"status":"error","message":"no file uploaded"}), 400

# #     # save locally (for quick testing)
# #     timestamp = datetime.utcnow().isoformat(timespec="seconds")
# #     local_name = f"uploads/{timestamp}_{file.filename}"
# #     os.makedirs("uploads", exist_ok=True)
# #     file.save(local_name)

# #     saved_to_gcs = False
# #     saved_to_firestore = False

# #     # attempt to save to GCS under demo_user/... or top-level uploads/
# #     if GCS_BUCKET:
# #         try:
# #             client = get_storage_client()
# #             bucket = client.bucket(GCS_BUCKET)
# #             blob_path = f"uploads/{timestamp}_{file.filename}"
# #             blob = bucket.blob(blob_path)
# #             # upload from local file
# #             blob.upload_from_filename(local_name)
# #             saved_to_gcs = True
# #         except Exception as e:
# #             app.logger.warning(f"GCS upload error: {e}")

# #     # optionally write a simple record to Firestore
# #     if FIRESTORE_ENABLED:
# #         try:
# #             db = get_firestore_client()
# #             doc_ref = db.collection("uploads").document()
# #             doc_ref.set({
# #                 "filename": file.filename,
# #                 "uploaded_at": datetime.utcnow(),
# #                 "gcs_path": blob_path if saved_to_gcs else None,
# #             })
# #             saved_to_firestore = True
# #         except Exception as e:
# #             app.logger.warning(f"Firestore write error: {e}")

# #     rows = 0
# #     try:
# #         # quick row count from local file
# #         with open(local_name, "r", encoding="utf-8") as fh:
# #             rows = sum(1 for _ in fh) - 1
# #             if rows < 0:
# #                 rows = 0
# #     except Exception:
# #         rows = 0

# #     return jsonify({
# #         "status":"ok",
# #         "firestore_saved": saved_to_firestore,
# #         "gcs_saved": saved_to_gcs,
# #         "rows": rows
# #     }), 202

# # # --- Health check root (not required but helpful) ---
# # @app.route("/", methods=["GET"])
# # def index():
# #     return jsonify({"service":"metabalance-agent","status":"ready","time":datetime.utcnow().isoformat()})

# # # ---------- Health & METRICS COMPUTATION LOGIC ----------
# # def safe_float(x):
# #     try:
# #         return float(x)
# #     except Exception:
# #         return None

# # def compute_whtr(waist_cm: float, height_cm: float) -> float:
# #     if not waist_cm or not height_cm:
# #         return None
# #     # WHtR = waist / height
# #     return round(waist_cm / height_cm, 3)

# # def whtr_band(whtr: float, gender: str = "") -> str:
# #     if whtr is None:
# #         return "unknown"
# #     # Simple bands: lower is better; these are general heuristics
# #     # Typical thresholds: <0.4: low, 0.4-0.5: healthy, 0.5-0.6: increased risk, >0.6 high
# #     if whtr < 0.42:
# #         return "low"
# #     if whtr < 0.5:
# #         return "healthy"
# #     if whtr < 0.6:
# #         return "increased_risk"
# #     return "high_risk"

# # def estimate_metabolic_age(metabolic_score: float, age: float) -> int:
# #     """
# #     Heuristic: metabolic score baseline at 50 ~ real age.
# #     metabolic_score > 50 -> younger metabolic age, <50 -> older
# #     """
# #     if metabolic_score is None or age is None:
# #         return None
# #     try:
# #         # each 5 points -> 1 year difference (simple)
# #         diff = (50 - metabolic_score) / 5.0
# #         estimated = round(age + diff)
# #         return int(max(10, min(100, estimated)))
# #     except Exception:
# #         return None

# # def compute_metabolic_score(metrics: Dict[str, Any]) -> int:
# #     """
# #     Simple score combining activity, sleep, waist/BMI proxy, cravings.
# #     Returns 0-100 (higher = better).
# #     This is heuristic and not a medical claim.
# #     """
# #     # weights
# #     steps = metrics.get("avg_steps", 0) or 0
# #     sleep = metrics.get("avg_sleep", 7) or 7
# #     waist = metrics.get("avg_waist", None)
# #     weight = metrics.get("avg_weight", None)
# #     cravings = metrics.get("cravings_days", 0) or 0
# #     alcohol_days = metrics.get("alcohol_days", 0) or 0

# #     # normalize components
# #     step_score = min(1.0, steps / 10000.0)  # 10k steps -> 1
# #     sleep_score = max(0.0, min(1.0, (sleep - 5) / 3.0))  # 5-8 hrs -> scale
# #     craving_penalty = min(1.0, cravings / 7.0)  # 7 means worst
# #     alcohol_penalty = min(1.0, alcohol_days / 7.0)

# #     waist_score = 0.5
# #     if waist:
# #         # smaller waist contributes positively (heuristic)
# #         waist_score = max(0.0, min(1.0, 1 - (waist - 80) / 40.0))

# #     # aggregate weighted
# #     score = (0.35 * step_score + 0.25 * sleep_score + 0.2 * waist_score + 0.1 * (1 - craving_penalty) + 0.1 * (1 - alcohol_penalty)) * 100
# #     score = int(round(max(0, min(100, score))))
# #     return score

# # # Hormonal heuristics (very simple heuristics for demo)
# # def female_hormonal_heuristics(rows: List[Dict[str, str]], metrics: Dict[str, Any]) -> Dict[str, Any]:
# #     """
# #     Very light-weight heuristics for menstrual cycle signals and PCOS-like patterns:
# #     - If cycle_phase indicated luteal + high cravings + low sleep -> flag PMS/PCOS-like pattern
# #     - High variability in cycle length or prolonged irregular cycles -> flag
# #     NOTE: these are heuristics only for demo, not a diagnosis.
# #     """
# #     flags = []
# #     # try to detect cycle_phase in latest row
# #     latest = rows[-1] if rows else {}
# #     cp = latest.get("cycle_phase", "").lower()
# #     avg_sleep = metrics.get("avg_sleep", 7)
# #     cravings_days = metrics.get("cravings_days", 0)

# #     if cp == "luteal" and cravings_days >= 4 and avg_sleep < 6.5:
# #         flags.append("pms_like_pattern_recommended_diet_and_sleep")

# #     # check cycle length variability
# #     cycle_lens = []
# #     for r in rows:
# #         v = r.get("cycle_length")
# #         if v:
# #             try:
# #                 cycle_lens.append(float(v))
# #             except Exception:
# #                 pass
# #     if cycle_lens:
# #         avg_len = sum(cycle_lens) / len(cycle_lens)
# #         if avg_len < 21 or avg_len > 35:
# #             flags.append("irregular_cycle_length_consult_specialist")
# #         if max(cycle_lens) - min(cycle_lens) > 7:
# #             flags.append("high_variability_in_cycle_length_track_more")

# #     return {"female_flags": flags}

# # def male_hormonal_heuristics(rows: List[Dict[str, str]], metrics: Dict[str, Any]) -> Dict[str, Any]:
# #     """
# #     Male hormonal heuristics:
# #     - central obesity (WHtR) + low steps + low sleep -> metabolic/hormonal concern hint (demo only)
# #     """
# #     flags = []
# #     whtr = metrics.get("whtr")
# #     avg_steps = metrics.get("avg_steps", 0)
# #     avg_sleep = metrics.get("avg_sleep", 7)

# #     if whtr and whtr >= 0.55:
# #         if avg_steps < 5000 or avg_sleep < 6.0:
# #             flags.append("possible_metabolic_risk_consider_lifestyle_change")
# #         else:
# #             flags.append("visceral_fat_risk_monitor_waist_and_activity")
# #     return {"male_flags": flags}

# # # --- CSV parsing helper: compute simple metrics from rows ---
# # def summarize_rows(rows: List[Dict[str, str]]) -> Dict[str, Any]:
# #     metrics = {
# #         "rows": len(rows),
# #         "avg_steps": 0,
# #         "avg_sleep": 0.0,
# #         "avg_waist": None,
# #         "avg_weight": None,
# #         "cravings_days": 0,
# #         "alcohol_days": 0,
# #         "height_cm": None,
# #         "age": None,
# #     }
# #     if not rows:
# #         return metrics

# #     steps_total = 0
# #     sleep_total = 0.0
# #     waist_vals = []
# #     weight_vals = []
# #     cravings_count = 0
# #     alcohol_count = 0
# #     heights = []
# #     ages = []

# #     for r in rows:
# #         s = safe_float(r.get("steps") or r.get("step_count") or 0) or 0
# #         steps_total += s
# #         sl = safe_float(r.get("sleep_hours") or r.get("sleep") or 0) or 0
# #         sleep_total += sl
# #         w = safe_float(r.get("waist_cm") or r.get("waist") or 0)
# #         if w:
# #             waist_vals.append(w)
# #         wt = safe_float(r.get("weight_kg") or r.get("weight") or 0)
# #         if wt:
# #             weight_vals.append(wt)
# #         if (r.get("cravings") or "").strip().lower() in ("1","yes","true","y","yday","craving"):
# #             cravings_count += 1
# #         # allow numeric cravings flag or text
# #         if (r.get("cravings_flag") or "").strip().lower() in ("1","true","yes","y"):
# #             cravings_count += 1
# #         if (r.get("alcohol") or "").strip().lower() in ("1","yes","true","y"):
# #             alcohol_count += 1
# #         h = safe_float(r.get("height_cm") or r.get("height") or 0)
# #         if h:
# #             heights.append(h)
# #         a = safe_float(r.get("age") or 0)
# #         if a:
# #             ages.append(a)

# #     metrics["avg_steps"] = int(round(steps_total / len(rows)))
# #     metrics["avg_sleep"] = round(sleep_total / len(rows), 2)
# #     if waist_vals:
# #         metrics["avg_waist"] = round(sum(waist_vals) / len(waist_vals), 2)
# #         metrics["latest_waist"] = waist_vals[-1]
# #         metrics["max_waist"] = max(waist_vals)
# #     else:
# #         metrics["avg_waist"] = None
# #     if weight_vals:
# #         metrics["avg_weight"] = round(sum(weight_vals) / len(weight_vals), 2)
# #         metrics["latest_weight"] = weight_vals[-1]
# #         metrics["max_weight"] = max(weight_vals)
# #     else:
# #         metrics["avg_weight"] = None
# #     metrics["cravings_days"] = cravings_count
# #     metrics["alcohol_days"] = alcohol_count
# #     metrics["height_cm"] = heights[-1] if heights else None
# #     metrics["age"] = ages[-1] if ages else None

# #     return metrics

# # # --- The main /run endpoint ---
# # @app.route("/run", methods=["POST"])
# # def run_agent():
# #     """
# #     Expected JSON body:
# #     {
# #       "user_id": "demo_user",
# #       "gcs_path": "demo_user/2025-11-16T18:00:51.039046_seven_day.csv",
# #       "gender": "female" or "male" or "", (optional)
# #       "age": 28 (optional)
# #     }
# #     """
# #     payload = request.get_json(force=True)
# #     user_id = payload.get("user_id", "anonymous")
# #     gcs_path = payload.get("gcs_path")
# #     gender = (payload.get("gender") or "").lower()
# #     provided_age = payload.get("age")

# #     if not gcs_path:
# #         return jsonify({"status":"error","error":"gcs_path required"}), 400

# #     # Read CSV from GCS
# #     try:
# #         rows = read_csv_from_gcs(gcs_path)
# #     except FileNotFoundError as e:
# #         return jsonify({"status":"error","error":"failed to read csv","details":str(e)}), 400
# #     except Exception as e:
# #         return jsonify({"status":"error","error":"failed to read csv","details":str(e)}), 500

# #     # Summarize rows to metrics
# #     metrics = summarize_rows(rows)

# #     # if provided age override
# #     if provided_age:
# #         try:
# #             metrics["age"] = float(provided_age)
# #         except Exception:
# #             pass

# #     # compute WHtR
# #     if metrics.get("avg_waist") and metrics.get("height_cm"):
# #         whtr = compute_whtr(metrics["avg_waist"], metrics["height_cm"])
# #     else:
# #         # if height not provided, attempt to infer from rows or default
# #         whtr = compute_whtr(metrics.get("latest_waist") or metrics.get("avg_waist") or None, metrics.get("height_cm") or None)

# #     metrics["whtr"] = whtr
# #     metrics["whtr_band"] = whtr_band(whtr)

# #     # compute metabolic score
# #     metabolic_score = compute_metabolic_score(metrics)
# #     metrics["metabolic_score"] = metabolic_score

# #     # estimate metabolic age
# #     metabolic_age = estimate_metabolic_age(metabolic_score, metrics.get("age") or provided_age or 30)
# #     metrics["metabolic_age"] = metabolic_age

# #     # heuristics by gender
# #     heuristics = {}
# #     if gender == "female" or gender == "f":
# #         heuristics.update(female_hormonal_heuristics(rows, metrics))
# #     elif gender == "male" or gender == "m":
# #         heuristics.update(male_hormonal_heuristics(rows, metrics))
# #     else:
# #         # include both sets as generic advice
# #         heuristics.update(female_hormonal_heuristics(rows, metrics))
# #         heuristics.update(male_hormonal_heuristics(rows, metrics))

# #     # Build an LLM-friendly summary text (placeholder — replace with Gemini call later)
# #     llm_text = f"Summary: Detected WHtR {metrics.get('whtr')} ({metrics.get('whtr_band')}).\nMetabolic score: {metrics.get('metabolic_score')}. Metabolic age estimate: {metrics.get('metabolic_age')}."
# #     # Wrap into a response doc
# #     summary_doc = {
# #         "user_id": user_id,
# #         "created_at": datetime.utcnow().isoformat(),
# #         "metrics": metrics,
# #         "llm_text": llm_text,
# #         "heuristics": heuristics
# #     }

# #     # Attempt to write the summary into Firestore for persistence
# #     firestore_saved = False
# #     if FIRESTORE_ENABLED:
# #         try:
# #             db = get_firestore_client()
# #             # store under collection metabalance_users/{user_id}/summaries
# #             doc_ref = db.collection("metabalance_users").document(user_id).collection("summaries").document()
# #             # Use Firestore native timestamp for created_at
# #             doc_ref.set({
# #                 "created_at": firestore.SERVER_TIMESTAMP,
# #                 "payload": summary_doc
# #             })
# #             firestore_saved = True
# #         except Exception as e:
# #             app.logger.warning(f"Firestore write error: {e}")

# #     # Return JSON result
# #     out = {
# #         "status": "ok",
# #         "summary": summary_doc,
# #         "firestore_saved": firestore_saved
# #     }
# #     # ensure JSON-serializable (convert datetime)
# #     return jsonify(out), 200

# # # --- Main entry: bind to PORT and 0.0.0.0 for Cloud Run ---
# # if __name__ == "__main__":
# #     # ensure uploads folder exists for local testing
# #     os.makedirs("uploads", exist_ok=True)
# #     # bind to 0.0.0.0 and read PORT env
# #     app.run(host="0.0.0.0", port=PORT)
# # agent.py
# import os
# import io
# import json
# import traceback
# from datetime import datetime
# from flask import Flask, request, jsonify
# import pandas as pd
# import requests

# # Google clients
# from google.cloud import storage
# from google.cloud import firestore

# app = Flask(__name__)

# # Configuration via env
# PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT") or os.environ.get("PROJECT_ID")
# FIRESTORE_ENABLED = os.environ.get("FIRESTORE_ENABLED", "false").lower() == "true"
# DEFAULT_GCS_BUCKET = os.environ.get("GCS_BUCKET", None)
# # Note: GEMINI_API_KEY is mapped into the Cloud Run container by Secret Manager using --set-secrets
# GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# # Initialize clients
# storage_client = None
# fs = None
# try:
#     storage_client = storage.Client()
# except Exception as e:
#     print("Warning: storage client init failed:", e)

# if FIRESTORE_ENABLED:
#     try:
#         fs = firestore.Client()
#     except Exception as e:
#         print("Warning: firestore client init failed:", e)


# # ----------------------------
# # Analysis helpers (same heuristics as server)
# # ----------------------------
# def safe_float(x, default=None):
#     try:
#         return float(x)
#     except Exception:
#         return default

# def compute_basic_metrics(df: pd.DataFrame):
#     out = {}
#     if df is None or df.shape[0] == 0:
#         return out
#     df = df.copy()
#     df.columns = [c.strip() for c in df.columns]
#     for col in ["steps", "sleep_hours", "waist_cm", "weight_kg", "height_cm", "age", "calories"]:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#     out["rows"] = len(df)
#     out["avg_steps"] = int(df["steps"].mean()) if "steps" in df.columns and not df["steps"].isna().all() else None
#     out["avg_sleep"] = round(float(df["sleep_hours"].mean()), 2) if "sleep_hours" in df.columns and not df["sleep_hours"].isna().all() else None
#     out["avg_waist"] = round(float(df["waist_cm"].mean()), 2) if "waist_cm" in df.columns and not df["waist_cm"].isna().all() else None
#     out["avg_weight"] = round(float(df["weight_kg"].mean()), 2) if "weight_kg" in df.columns and not df["weight_kg"].isna().all() else None
#     out["max_waist"] = float(df["waist_cm"].max()) if "waist_cm" in df.columns else None
#     out["max_weight"] = float(df["weight_kg"].max()) if "weight_kg" in df.columns else None
#     out["latest_waist"] = float(df["waist_cm"].iloc[-1]) if "waist_cm" in df.columns else None
#     out["latest_weight"] = float(df["weight_kg"].iloc[-1]) if "weight_kg" in df.columns else None
#     out["height_cm"] = float(df["height_cm"].iloc[-1]) if "height_cm" in df.columns and not df["height_cm"].isna().all() else None
#     out["age"] = float(df["age"].iloc[-1]) if "age" in df.columns and not df["age"].isna().all() else None
#     out["cravings_days"] = int(df["cravings"].astype(bool).sum()) if "cravings" in df.columns else 0
#     out["alcohol_days"] = int(df["alcohol_intake"].astype(bool).sum()) if "alcohol_intake" in df.columns else 0
#     return out

# def compute_whtr(latest_waist_cm, height_cm):
#     if latest_waist_cm is None or height_cm is None:
#         return None
#     try:
#         return round(latest_waist_cm / height_cm, 3)
#     except Exception:
#         return None

# def whtr_band(whtr):
#     if whtr is None:
#         return "unknown"
#     if whtr < 0.5:
#         return "healthy"
#     if whtr < 0.6:
#         return "elevated"
#     return "high"

# def compute_metabolic_score(metrics: dict):
#     score = 50
#     if metrics.get("avg_steps") is not None:
#         steps = metrics["avg_steps"]
#         score += min(20, int((steps / 8000) * 20))
#     if metrics.get("avg_sleep") is not None:
#         sleep = metrics["avg_sleep"]
#         if 7 <= sleep <= 8:
#             score += 15
#         elif 6 <= sleep < 7 or 8 < sleep <= 9:
#             score += 8
#     score -= min(10, metrics.get("cravings_days", 0) * 2)
#     score -= min(10, metrics.get("alcohol_days", 0) * 2)
#     if metrics.get("whtr") is not None:
#         band = metrics.get("whtr_band")
#         if band == "elevated":
#             score -= 5
#         elif band == "high":
#             score -= 12
#     score = max(1, min(99, int(score)))
#     return score

# def metabolic_age_estimate(score, chronological_age=None):
#     if score is None:
#         return None
#     if chronological_age is None:
#         chronological_age = 30
#     delta = (score - 50) / 10
#     return max(16, int(round(chronological_age - delta)))

# def female_hormonal_insights(df):
#     hints = []
#     if df is None or df.shape[0] == 0:
#         return hints
#     if "cycle_phase" in df.columns:
#         last_phase = str(df["cycle_phase"].iloc[-1]).lower()
#         if "luteal" in last_phase or "pms" in last_phase:
#             low_sleep = ("sleep_hours" in df.columns) and (df["sleep_hours"].iloc[-1] < 6.5)
#             cravings = ("cravings" in df.columns) and bool(df["cravings"].iloc[-1])
#             if cravings and low_sleep:
#                 hints.append("Pattern suggests luteal-phase + cravings + low sleep — monitor for PMS/PCOD-like patterns; stabilize carbs and protein.")
#     if "waist_cm" in df.columns:
#         if df["waist_cm"].iloc[-1] - df["waist_cm"].mean() > 3:
#             hints.append("Recent waist increase >3cm from your average; consider tracking meals and alcohol intake.")
#     return hints

# def male_hormonal_insights(metrics, df=None):
#     hints = []
#     if metrics is None:
#         return hints
#     if metrics.get("whtr") is not None:
#         if metrics["whtr"] >= 0.55:
#             hints.append("WHtR indicates elevated visceral fat risk; this pattern often correlates with lower testosterone in males.")
#     if metrics.get("avg_steps", 0) < 5000:
#         hints.append("Low average daily steps — increasing daily activity helps reduce visceral fat and improve hormonal profiles.")
#     if metrics.get("avg_sleep", 8) < 6.5:
#         hints.append("Short sleep may increase cortisol and negatively affect testosterone and metabolic health.")
#     return hints

# # ----------------------------
# # GCS read helper
# # ----------------------------
# def read_csv_from_gcs(bucket_name, blob_name):
#     if storage_client is None:
#         raise RuntimeError("Storage client not initialized")
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(blob_name)
#     if not blob.exists():
#         raise FileNotFoundError(f"No such object: {bucket_name}/{blob_name}")
#     data = blob.download_as_bytes()
#     df = pd.read_csv(io.BytesIO(data))
#     return df

# # ----------------------------
# # Gemini integration (HTTP)
# # ----------------------------
# def call_gemini_http(prompt: str, model: str = "gemini-1.5"):
#     """
#     Call the Gemini REST endpoint with the API key in env GEMINI_API_KEY.
#     Returns text (string) on success, or None on failure.
#     """
#     if not GEMINI_API_KEY:
#         print("DEBUG: GEMINI_API_KEY not set in env")
#         return None

#     # NOTE: endpoint and request shape may vary by API version. This is a generic attempt.
#     url = f"https://api.generative.google/v1beta2/models/{model}:generate"
#     headers = {
#         "Authorization": f"Bearer {GEMINI_API_KEY}",
#         "Content-Type": "application/json",
#     }
#     body = {
#         "prompt": {"text": prompt},
#         "maxOutputTokens": 400
#     }
#     try:
#         resp = requests.post(url, headers=headers, json=body, timeout=30)
#         resp.raise_for_status()
#         data = resp.json()
#         # Try to extract natural language text from common shapes
#         text = None
#         if isinstance(data, dict):
#             if "candidates" in data and isinstance(data["candidates"], list) and len(data["candidates"]) > 0:
#                 cand = data["candidates"][0]
#                 text = cand.get("content") or cand.get("text") or cand.get("output")
#             elif "output" in data:
#                 # some variants place the text here
#                 text = data.get("output")
#         if not text:
#             text = json.dumps(data)
#         return text
#     except Exception as e:
#         print("Gemini call failed:", e)
#         try:
#             print("Response text:", e.response.text)  # may not always exist
#         except Exception:
#             pass
#         return None

# # ----------------------------
# # Firestore helper
# # ----------------------------
# def save_agent_summary(user_id, summary):
#     if not FIRESTORE_ENABLED or fs is None:
#         return False
#     try:
#         fs.collection("metabalance_users").document(user_id).collection("summaries").document().set(summary)
#         # also save raw agent call entry
#         fs.collection("metabalance_users").document(user_id).collection("agent_calls").add({
#             "agent_summary": summary,
#             "called_at": datetime.utcnow().isoformat()
#         })
#         return True
#     except Exception as e:
#         print("Firestore write error:", e)
#         return False

# # ----------------------------
# # Flask route: /run
# # ----------------------------
# @app.route("/run", methods=["POST"])
# def run_agent():
#     """
#     Expects JSON:
#     {
#       "user_id": "demo_user",
#       "gender": "female" | "male" | "mixed",
#       "gcs_path": "demo_user/..._file.csv",
#       "gcs_bucket": "optional-bucket-name"   # if omitted, uses DEFAULT_GCS_BUCKET env
#     }
#     """
#     try:
#         payload = request.get_json(force=True)
#         if not payload:
#             return jsonify({"status":"error","error":"invalid json"}), 400

#         user_id = payload.get("user_id", "demo_user")
#         gender = payload.get("gender", "female")
#         gcs_path = payload.get("gcs_path")
#         bucket = payload.get("gcs_bucket") or DEFAULT_GCS_BUCKET
#         if not gcs_path:
#             return jsonify({"status":"error","error":"gcs_path required"}), 400
#         if not bucket:
#             return jsonify({"status":"error","error":"GCS bucket not configured"}), 400

#         # Read CSV from GCS
#         try:
#             df = read_csv_from_gcs(bucket, gcs_path)
#         except FileNotFoundError as fe:
#             return jsonify({"status":"error","error": str(fe)}), 400
#         except Exception as e:
#             print("CSV read error:", e)
#             return jsonify({"status":"error","error":"failed to read csv", "exception": str(e)}), 500

#         # compute metrics
#         metrics = compute_basic_metrics(df)
#         metrics["whtr"] = compute_whtr(metrics.get("latest_waist"), metrics.get("height_cm"))
#         metrics["whtr_band"] = whtr_band(metrics["whtr"]) if metrics.get("whtr") is not None else "unknown"
#         metrics["metabolic_score"] = compute_metabolic_score(metrics)
#         metrics["metabolic_age"] = metabolic_age_estimate(metrics["metabolic_score"], metrics.get("age"))
#         metrics["rows"] = metrics.get("rows", 0)

#         # heuristics
#         heuristics = {}
#         if gender and gender.lower() == "female":
#             heuristics["female_flags"] = female_hormonal_insights(df)
#             heuristics["male_flags"] = []
#         elif gender and gender.lower() == "male":
#             heuristics["male_flags"] = male_hormonal_insights(metrics, df)
#             heuristics["female_flags"] = []
#         else:
#             heuristics["female_flags"] = female_hormonal_insights(df)
#             heuristics["male_flags"] = male_hormonal_insights(metrics, df)

#         # build prompt for Gemini (concise)
#         prompt = (
#             f"MetaBalance: produce a concise, non-medical, user-facing summary.\n"
#             f"User: {user_id}\n"
#             f"Metrics: WHtR={metrics.get('whtr')}, WHtR_band={metrics.get('whtr_band')}, "
#             f"MetabolicScore={metrics.get('metabolic_score')}, MetabolicAge={metrics.get('metabolic_age')}, "
#             f"AvgSteps={metrics.get('avg_steps')}, AvgSleep={metrics.get('avg_sleep')}\n"
#             f"Heuristics: {heuristics}\n\n"
#             "Output:\n"
#             "- Provide exactly 3 short actionable bullet points (one sentence each).\n"
#             "- Provide a one-line concise conclusion.\n"
#             "- Use plain language and avoid medical diagnoses. Keep output <= 200 words."
#         )

#         # Attempt Gemini
#         llm_text = None
#         gemini_output = None
#         try:
#             gemini_output = call_gemini_http(prompt)
#             if gemini_output:
#                 llm_text = gemini_output.strip()
#                 print("DEBUG: Gemini produced output (len):", len(llm_text))
#             else:
#                 print("DEBUG: Gemini returned no text; falling back to heuristic summary.")
#         except Exception as e:
#             print("DEBUG: Gemini exception:", e)
#             print(traceback.format_exc())

#         # Fallback heuristic summary if Gemini failed
#         if not llm_text:
#             llm_text = (
#                 "Summary: Detected WHtR {} ({}). Metabolic score: {}. Metabolic age estimate: {}."
#                 .format(metrics.get("whtr") if metrics.get("whtr") is not None else "unknown",
#                         metrics.get("whtr_band"),
#                         metrics.get("metabolic_score"),
#                         metrics.get("metabolic_age"))
#             )

#         # final summary object
#         summary = {
#             "created_at": datetime.utcnow().isoformat(),
#             "user_id": user_id,
#             "gcs_path": gcs_path,
#             "metrics": metrics,
#             "heuristics": heuristics,
#             "llm_text": llm_text
#         }

#         # save to firestore
#         fs_saved = False
#         try:
#             fs_saved = save_agent_summary(user_id, summary)
#         except Exception as e:
#             print("Failed to save summary to Firestore:", e)

#         return jsonify({"status":"ok", "firestore_saved": fs_saved, "summary": summary}), 200

#     except Exception as e:
#         print("Agent error:", e)
#         print(traceback.format_exc())
#         return jsonify({"status":"error","error": str(e)}), 500


# # Health endpoint
# @app.route("/healthz", methods=["GET"])
# def health():
#     return jsonify({"status":"ok","time": datetime.utcnow().isoformat()}), 200


# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 8080))
#     print("Starting agent on port", port, "Firestore enabled:", FIRESTORE_ENABLED, "Default bucket:", DEFAULT_GCS_BUCKET)
#     app.run(host="0.0.0.0", port=port)


# agent.py
import os
import io
import json
import logging
import traceback
from datetime import datetime

import pandas as pd
import requests
from flask import Flask, request, jsonify

# Google libs
from google.cloud import storage, firestore

# Configuration
LOG = logging.getLogger("agent")
logging.basicConfig(level=logging.INFO)

GCS_BUCKET = os.environ.get("GCS_BUCKET")
FIRESTORE_ENABLED = os.environ.get("FIRESTORE_ENABLED", "false").lower() in ("1", "true", "yes")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")  # set via Secret Manager in Cloud Run

app = Flask(__name__)

# Initialize clients lazily (so local dev can run without project default credentials until needed)
_storage_client = None
_firestore_client = None


def get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


def get_firestore_client():
    global _firestore_client
    if _firestore_client is None:
        _firestore_client = firestore.Client()
    return _firestore_client


# ---------------------------
# Utilities & heuristics
# ---------------------------

def safe_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def series_to_scalar(s):
    """Return single scalar if series length==1, else None."""
    if s is None:
        return None
    if isinstance(s, pd.Series):
        if s.size == 0:
            return None
        if s.size == 1:
            return safe_float(s.iloc[0])
        # caller should handle arrays
        return None
    return safe_float(s)


def whtr_from_height_waist(height_cm, waist_cm):
    """Waist-to-height ratio (WHtR)."""
    if height_cm is None or waist_cm is None:
        return None
    try:
        h = float(height_cm)
        w = float(waist_cm)
        if h <= 0:
            return None
        return w / h
    except Exception:
        return None


def whtr_band(whtr):
    if whtr is None:
        return None
    # basic bands (adult)
    if whtr < 0.42:
        return "very low"
    if whtr < 0.48:
        return "healthy"
    if whtr < 0.53:
        return "increased risk"
    return "high risk"


def metabolic_score_from_metrics(metrics):
    """
    Simple heuristic metabolic score (0-100):
    - steps (target 7000), sleep (target 7), whtr (lower is better)
    - Weights are illustrative and simple for hackathon demo.
    """
    score = 50
    steps = metrics.get("avg_steps")
    sleep = metrics.get("avg_sleep")
    whtr = metrics.get("whtr")
    weight = metrics.get("avg_weight")

    # Steps contribution
    if steps is not None:
        try:
            s = float(steps)
            score += min(20, (s / 7000.0) * 20)  # up to +20
        except:
            pass

    # Sleep contribution
    if sleep is not None:
        try:
            sl = float(sleep)
            # ideal 7 -> +10, very low -> -10
            score += max(-10, min(10, (sl - 7.0) * 3))
        except:
            pass

    # WHtR penalty
    if whtr is not None:
        try:
            w = float(whtr)
            if w < 0.48:
                score += 10
            elif w < 0.53:
                score += 0
            else:
                score -= 15
        except:
            pass

    # Bound and convert
    score = max(0, min(100, int(round(score))))
    # Metabolic age: simple mapping: higher score -> lower metabolic age compared to chronological
    metabolic_age = None
    age = metrics.get("age")
    if age is not None:
        try:
            a = float(age)
            # delta: score 100 -> metabolic_age = age - 8, score 50 -> same as age, score 0 -> age + 8
            metab_offset = (50 - score) / 50 * 8
            metabolic_age = max(12, int(round(a + metab_offset)))
        except:
            metabolic_age = None

    return int(score), metabolic_age


def heuristics_from_df(df: pd.DataFrame):
    """
    Input: a dataframe parsed from CSV (7-day or single row).
    Attempts to find columns (case-insensitive) for:
      - weight, weight_kg, body_weight
      - waist, waist_cm, waist_cm
      - steps (daily steps), sleep_hours or sleep
      - cravings, alcohol
      - height_cm, age
    Returns: metrics dict and heuristic flags.
    """
    metrics = {}
    flags = {}

    # lower-case column mapping
    lc_map = {c.lower(): c for c in df.columns}

    def series_or_none(*names):
        for n in names:
            if n.lower() in lc_map:
                s = df[lc_map[n.lower()]]
                if isinstance(s, pd.Series):
                    return s
        return None

    # weight series
    weight_s = series_or_none("weight", "weight_kg", "body_weight")
    waist_s = series_or_none("waist", "waist_cm", "waist_cms", "waist_cm.")
    steps_s = series_or_none("steps", "daily_steps", "step_count")
    sleep_s = series_or_none("sleep", "sleep_hours", "avg_sleep")
    cravings_s = series_or_none("cravings", "craving", "high_cravings")
    alcohol_s = series_or_none("alcohol", "alcohol_days")
    height_s = series_or_none("height_cm", "height", "stature_cm")
    age_s = series_or_none("age")

    # scalar conversions (for per-day we compute averages)
    try:
        if weight_s is not None and weight_s.size > 0:
            metrics["avg_weight"] = float(pd.to_numeric(weight_s, errors="coerce").dropna().mean())
            metrics["max_weight"] = float(pd.to_numeric(weight_s, errors="coerce").dropna().max())
            metrics["latest_weight"] = float(pd.to_numeric(weight_s, errors="coerce").dropna().iloc[-1])
        else:
            metrics["avg_weight"] = None
    except Exception:
        metrics["avg_weight"] = None

    try:
        if waist_s is not None and waist_s.size > 0:
            metrics["avg_waist"] = float(pd.to_numeric(waist_s, errors="coerce").dropna().mean())
            metrics["max_waist"] = float(pd.to_numeric(waist_s, errors="coerce").dropna().max())
            metrics["latest_waist"] = float(pd.to_numeric(waist_s, errors="coerce").dropna().iloc[-1])
        else:
            metrics["avg_waist"] = None
    except Exception:
        metrics["avg_waist"] = None

    # steps / sleep averages
    try:
        if steps_s is not None and steps_s.size > 0:
            metrics["avg_steps"] = float(pd.to_numeric(steps_s, errors="coerce").dropna().mean())
        else:
            metrics["avg_steps"] = None
    except Exception:
        metrics["avg_steps"] = None

    try:
        if sleep_s is not None and sleep_s.size > 0:
            metrics["avg_sleep"] = float(pd.to_numeric(sleep_s, errors="coerce").dropna().mean())
        else:
            metrics["avg_sleep"] = None
    except Exception:
        metrics["avg_sleep"] = None

    try:
        if cravings_s is not None and cravings_s.size > 0:
            metrics["cravings_days"] = int(pd.to_numeric(cravings_s, errors="coerce").dropna().sum())
        else:
            metrics["cravings_days"] = 0
    except Exception:
        metrics["cravings_days"] = 0

    try:
        if alcohol_s is not None and alcohol_s.size > 0:
            metrics["alcohol_days"] = int(pd.to_numeric(alcohol_s, errors="coerce").dropna().sum())
        else:
            metrics["alcohol_days"] = 0
    except Exception:
        metrics["alcohol_days"] = 0

    # height and age (single values often)
    metrics["height_cm"] = None
    if height_s is not None:
        # try last non-null
        try:
            nonnull = pd.to_numeric(height_s, errors="coerce").dropna()
            if not nonnull.empty:
                metrics["height_cm"] = float(nonnull.iloc[-1])
        except:
            metrics["height_cm"] = None

    metrics["age"] = None
    if age_s is not None:
        try:
            nonnull = pd.to_numeric(age_s, errors="coerce").dropna()
            if not nonnull.empty:
                metrics["age"] = float(nonnull.iloc[-1])
        except:
            metrics["age"] = None

    # compute WHtR if possible
    if metrics.get("height_cm") and metrics.get("avg_waist"):
        whtr = whtr_from_height_waist(metrics["height_cm"], metrics["avg_waist"])
    elif metrics.get("height_cm") and metrics.get("latest_waist"):
        whtr = whtr_from_height_waist(metrics["height_cm"], metrics["latest_waist"])
    else:
        whtr = None
    metrics["whtr"] = whtr
    metrics["whtr_band"] = whtr_band(whtr)

    # compute metabolic score & metabolic age
    metabolic_score, metabolic_age = metabolic_score_from_metrics({
        "avg_steps": metrics.get("avg_steps"),
        "avg_sleep": metrics.get("avg_sleep"),
        "whtr": metrics.get("whtr"),
        "age": metrics.get("age"),
        "avg_weight": metrics.get("avg_weight")
    })
    metrics["metabolic_score"] = metabolic_score
    metrics["metabolic_age"] = metabolic_age

    # simple heuristics for female hormonal patterns (demo-level)
    female_flags = []
    if metrics.get("avg_sleep") is not None and metrics.get("cravings_days", 0) > 0:
        if metrics["avg_sleep"] < 6.5 and metrics["cravings_days"] >= 2:
            female_flags.append("low_sleep_high_cravings_possible_hormonal_pattern")

    # male flags - e.g., central adiposity concern
    male_flags = []
    if metrics.get("whtr") is not None:
        if metrics["whtr"] >= 0.53:
            male_flags.append("elevated_whtr_central_adiposity")

    flags["female_flags"] = female_flags
    flags["male_flags"] = male_flags

    # return
    return metrics, flags


# ---------------------------
# Gemini LLM call (official generateContent)
# ---------------------------

def call_gemini(prompt_text: str) -> str:

    api_key = GEMINI_API_KEY or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        LOG.info("GEMINI_API_KEY not set; skipping LLM.")
        return "LLM skipped: GEMINI_API_KEY not set."

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"

    payload = {
        "contents": [
            {"parts": [{"text": prompt_text}]}
        ]
    }

    headers = {"Content-Type": "application/json"}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        # extract safely; the exact path may vary by API version
        try:
            return data["candidates"][0]["content"][0]["parts"][0]["text"]
        except Exception:
            return json.dumps(data)
    except Exception as e:
        LOG.exception("Gemini call failed")
        return f"LLM call failed: {e}"


# ---------------------------
# GCS / Firestore helpers
# ---------------------------

def read_csv_from_gcs(gcs_path: str):
    """
    gcs_path expected like: "prefix/file.csv" (relative path inside bucket)
    """
    client = get_storage_client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        raise FileNotFoundError(f"No such object: {GCS_BUCKET}/{gcs_path}")
    data = blob.download_as_text()
    # try to parse with pandas
    df = pd.read_csv(io.StringIO(data))
    return df


def write_summary_to_firestore(user_id: str, summary: dict):
    if not FIRESTORE_ENABLED:
        LOG.info("Firestore disabled by env; skipping write.")
        return False
    try:
        db = get_firestore_client()
        coll = db.collection("metabalance_summaries")
        doc_id = f"{user_id}_{datetime.utcnow().isoformat()}"
        coll.document(doc_id).set(summary)
        return True
    except Exception:
        LOG.exception("Firestore write error")
        return False


# ---------------------------
# Build LLM prompt (simple)
# ---------------------------

def build_llm_prompt(user_id: str, metrics: dict, flags: dict):
    lines = []
    lines.append(f"MetaBalance AI summary for user {user_id}:")
    lines.append(json.dumps(metrics, default=str, indent=2))
    lines.append("Heuristics/flags:")
    lines.append(json.dumps(flags, default=str, indent=2))
    lines.append("Write a concise 3-bullet personalized summary and 3 practical recommendations to improve metabolic health.")
    return "\n\n".join(lines)


# ---------------------------
# Flask endpoints
# ---------------------------

@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200


@app.route("/run", methods=["POST"])
def run_agent():
    """
    Exposed endpoint used by Cloud Run and server to trigger agent processing.
    Body JSON: {user_id, gender, gcs_path}
    """
    try:
        payload = request.get_json(force=True)
        user_id = payload.get("user_id")
        gender = (payload.get("gender") or "").lower()
        gcs_path = payload.get("gcs_path")

        LOG.info("run_agent called user=%s gender=%s gcs_path=%s", user_id, gender, gcs_path)
        if not user_id or not gcs_path:
            return jsonify({"status": "error", "error": "missing user_id or gcs_path"}), 400

        # read csv from GCS
        try:
            df = read_csv_from_gcs(gcs_path)
        except FileNotFoundError as e:
            return jsonify({"status": "error", "error": str(e), "details": "failed to read csv"}), 400
        except Exception as e:
            LOG.exception("Failed to read csv")
            return jsonify({"status": "error", "error": "failed to read csv", "details": str(e)}), 500

        # compute heuristics
        try:
            metrics, flags = heuristics_from_df(df)
            metrics["rows"] = int(df.shape[0])
        except Exception as e:
            LOG.exception("Heuristics failed")
            return jsonify({"status": "error", "error": "heuristics_failed", "details": str(e), "trace": traceback.format_exc()}), 500

        # Build LLM prompt and call if enabled
        prompt = build_llm_prompt(user_id, metrics, flags)
        llm_text = call_gemini(prompt)

        # Save summary to Firestore
        summary = {
            "user_id": user_id,
            "created_at": datetime.utcnow().isoformat(),
            "metrics": metrics,
            "heuristics": flags,
            "llm_text": llm_text
        }
        fs_written = write_summary_to_firestore(user_id, summary)

        # Return summary
        return jsonify({
            "status": "ok",
            "firestore_saved": bool(fs_written),
            "summary": summary
        }), 200

    except Exception as e:
        LOG.exception("Unhandled error in run_agent")
        return jsonify({"status": "error", "error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    # command line run for dev
    LOG.info("Starting MetaBalance agent (GCS_BUCKET=%s FIRESTORE_ENABLED=%s)", GCS_BUCKET, FIRESTORE_ENABLED)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))