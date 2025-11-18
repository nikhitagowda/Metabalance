import os
import io
import json
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory
import pandas as pd
import requests

# Google clients
from google.cloud import storage
from google.cloud import firestore

app = Flask(__name__)

# -------------------------
# Configuration (env)
# -------------------------
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
FIRESTORE_ENABLED = os.environ.get("FIRESTORE_ENABLED", "false").lower() == "true"
GCS_BUCKET = os.environ.get("GCS_BUCKET", "nikhita")
AGENT_URL = os.environ.get("AGENT_URL", "https://metabalance-agent-1010538041638.us-central1.run.app/run")

# Initialize clients (may fail locally if creds not set)
storage_client = None
fs = None
try:
    storage_client = storage.Client()
except Exception as e:
    print("Warning: storage client init failed:", e)

if FIRESTORE_ENABLED:
    try:
        fs = firestore.Client()
    except Exception as e:
        print("Warning: firestore client init failed:", e)

# -------------------------
# Analysis helpers
# -------------------------
def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def compute_basic_metrics(rows_df):
    out = {}
    if rows_df is None or rows_df.shape[0] == 0:
        return out
    df = rows_df.copy()
    df.columns = [c.strip() for c in df.columns]
    for col in ["steps", "sleep_hours", "waist_cm", "weight_kg", "height_cm", "age", "calories"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    out["rows"] = len(df)
    out["avg_steps"] = int(df["steps"].mean()) if "steps" in df.columns else None
    out["avg_sleep"] = round(float(df["sleep_hours"].mean()), 2) if "sleep_hours" in df.columns else None
    out["avg_waist"] = round(float(df["waist_cm"].mean()), 2) if "waist_cm" in df.columns else None
    out["avg_weight"] = round(float(df["weight_kg"].mean()), 2) if "weight_kg" in df.columns else None
    out["max_waist"] = float(df["waist_cm"].max()) if "waist_cm" in df.columns else None
    out["max_weight"] = float(df["weight_kg"].max()) if "weight_kg" in df.columns else None
    out["latest_waist"] = float(df["waist_cm"].iloc[-1]) if "waist_cm" in df.columns else None
    out["latest_weight"] = float(df["weight_kg"].iloc[-1]) if "weight_kg" in df.columns else None
    out["height_cm"] = float(df["height_cm"].iloc[-1]) if "height_cm" in df.columns and not df["height_cm"].isna().all() else None
    out["age"] = float(df["age"].iloc[-1]) if "age" in df.columns and not df["age"].isna().all() else None
    out["cravings_days"] = int(df["cravings"].astype(bool).sum()) if "cravings" in df.columns else 0
    out["alcohol_days"] = int(df["alcohol_intake"].astype(bool).sum()) if "alcohol_intake" in df.columns else 0
    return out

def compute_whtr(latest_waist_cm, height_cm):
    if not latest_waist_cm or not height_cm:
        return None
    try:
        return round(latest_waist_cm / height_cm, 3)
    except Exception:
        return None

def whtr_band(whtr):
    if whtr is None:
        return "unknown"
    if whtr < 0.5:
        return "healthy"
    if whtr < 0.6:
        return "elevated"
    return "high"

def compute_metabolic_score(metrics: dict):
    score = 50
    if metrics.get("avg_steps") is not None:
        steps = metrics["avg_steps"]
        score += min(20, int((steps / 8000) * 20))
    if metrics.get("avg_sleep") is not None:
        sleep = metrics["avg_sleep"]
        if 7 <= sleep <= 8:
            score += 15
        elif 6 <= sleep < 7 or 8 < sleep <= 9:
            score += 8
    score -= min(10, metrics.get("cravings_days", 0) * 2)
    score -= min(10, metrics.get("alcohol_days", 0) * 2)
    if metrics.get("whtr") is not None:
        band = metrics.get("whtr_band")
        if band == "elevated":
            score -= 5
        elif band == "high":
            score -= 12
    score = max(1, min(99, int(score)))
    return score

def metabolic_age_estimate(score, chronological_age=None):
    if score is None:
        return None
    if chronological_age is None:
        chronological_age = 30
    delta = (score - 50) / 10
    return max(16, int(round(chronological_age - delta)))

def female_hormonal_insights(df):
    hints = []
    if df is None or df.shape[0] == 0:
        return hints
    if "cycle_phase" in df.columns:
        last_phase = str(df["cycle_phase"].iloc[-1]).lower()
        if "luteal" in last_phase or "pms" in last_phase:
            low_sleep = ("sleep_hours" in df.columns) and (df["sleep_hours"].iloc[-1] < 6.5)
            cravings = ("cravings" in df.columns) and bool(df["cravings"].iloc[-1])
            if cravings and low_sleep:
                hints.append("Pattern suggests luteal-phase + cravings + low sleep — monitor for PMS/PCOD-like patterns; stabilize carbs and protein.")
    if "waist_cm" in df.columns:
        if df["waist_cm"].iloc[-1] - df["waist_cm"].mean() > 3:
            hints.append("Recent waist increase >3cm from your average; consider tracking meals and alcohol intake.")
    return hints

def male_hormonal_insights(metrics, df=None):
    hints = []
    if metrics is None:
        return hints
    if metrics.get("whtr") is not None:
        if metrics["whtr"] >= 0.55:
            hints.append("WHtR indicates elevated visceral fat risk; this pattern often correlates with lower testosterone in males.")
    if metrics.get("avg_steps", 0) < 5000:
        hints.append("Low average daily steps — increasing daily activity helps reduce visceral fat and improve hormonal profiles.")
    if metrics.get("avg_sleep", 8) < 6.5:
        hints.append("Short sleep may increase cortisol and negatively affect testosterone and metabolic health.")
    return hints

# -------------------------
# Storage / Firestore helpers
# -------------------------
def save_summary_to_firestore(user_id, summary):
    if not FIRESTORE_ENABLED or fs is None:
        return False
    try:
        col = fs.collection("metabalance_users").document(user_id).collection("summaries")
        col.document().set(summary)
        return True
    except Exception as e:
        print("Firestore write error:", e)
        return False

def upload_csv_to_gcs(local_path_or_fileobj, dest_blob_name):
    if storage_client is None:
        raise RuntimeError("Storage client not initialized")
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(dest_blob_name)
    if hasattr(local_path_or_fileobj, "read"):
        blob.upload_from_file(local_path_or_fileobj, content_type="text/csv")
    else:
        blob.upload_from_filename(local_path_or_fileobj, content_type="text/csv")
    return blob

# -------------------------
# Routes
# -------------------------
@app.route("/", methods=["GET"])
def index():
    # Prefer static/index.html if present, otherwise fall back to root index.html, otherwise simple string.
    static_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    root_path = os.path.join(os.path.dirname(__file__), "index.html")

    if os.path.exists(static_path):
        return send_from_directory("static", "index.html")
    elif os.path.exists(root_path):
        return send_from_directory(".", "index.html")
    else:
        return "MetaBalance AI - backend running"


@app.route("/api/upload", methods=["POST"])
def api_upload():
    try:
        if "file" not in request.files:
            return jsonify({"status":"error","error":"no file in request"}), 400
        f = request.files["file"]
        gender = request.form.get("gender", request.args.get("gender", "female"))
        user_id = request.form.get("user_id", request.args.get("user_id", "demo_user"))

        file_bytes = f.read()
        df = pd.read_csv(io.BytesIO(file_bytes))
        rows_count = df.shape[0]

        timestamp = datetime.utcnow().isoformat(timespec="seconds")
        original_name = f.filename or "uploaded.csv"
        base_name = original_name.replace(" ", "_")
        dest_name = f"{user_id}/{timestamp}_{base_name}"

        blob = upload_csv_to_gcs(io.BytesIO(file_bytes), dest_name)

        metrics = compute_basic_metrics(df)
        metrics["whtr"] = compute_whtr(metrics.get("latest_waist"), metrics.get("height_cm"))
        metrics["whtr_band"] = whtr_band(metrics["whtr"]) if metrics.get("whtr") else "unknown"
        metrics["metabolic_score"] = compute_metabolic_score(metrics)
        metrics["metabolic_age"] = metabolic_age_estimate(metrics["metabolic_score"], metrics.get("age"))
        metrics["rows"] = rows_count

        heuristics = {}
        if gender and gender.lower() == "female":
            heuristics["female_flags"] = female_hormonal_insights(df)
            heuristics["male_flags"] = []
        elif gender and gender.lower() == "male":
            heuristics["male_flags"] = male_hormonal_insights(metrics, df)
            heuristics["female_flags"] = []
        else:
            heuristics["female_flags"] = female_hormonal_insights(df)
            heuristics["male_flags"] = male_hormonal_insights(metrics, df)

        summary = {
            "created_at": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "gcs_path": dest_name,
            "metrics": metrics,
            "heuristics": heuristics
        }

        fs_saved = False
        if FIRESTORE_ENABLED:
            fs_saved = save_summary_to_firestore(user_id, summary)

        # DEBUG print (shows what we will call)
        agent_payload = {"user_id": user_id, "gender": gender, "gcs_path": dest_name}
        print("DEBUG: about to call agent_url:", AGENT_URL, "payload:", agent_payload)

        agent_response = None
        agent_ok = False
        try:
            r = requests.post(AGENT_URL, json=agent_payload, timeout=25)
            print("DEBUG: agent HTTP status:", r.status_code)
            if r.status_code == 200:
                agent_ok = True
                try:
                    agent_response = r.json()
                except Exception:
                    agent_response = {"text": r.text}
                # save agent call to Firestore (optional)
                if FIRESTORE_ENABLED and fs is not None:
                    try:
                        fs.collection("metabalance_users").document(user_id).collection("agent_calls").add({
                            "agent_response": agent_response,
                            "called_at": datetime.utcnow().isoformat(),
                            "gcs_path": dest_name
                        })
                    except Exception as e:
                        print("Failed to save agent call to Firestore:", e)
            else:
                agent_response = {"status":"error","http_status": r.status_code, "text": r.text}
        except Exception as e:
            agent_response = {"status":"error","exception": str(e)}
            print("DEBUG: agent call exception:", e)

        return jsonify({
            "status":"ok",
            "rows": rows_count,
            "gcs_path": dest_name,
            "firestore_saved": fs_saved,
            "agent_called": agent_ok,
            "agent_response": agent_response
        }), 202

    except Exception as e:
        print("Upload handler exception:", e)
        return jsonify({"status":"error","error": str(e)}), 500

@app.route("/api/summaries/<user_id>", methods=["GET"])
def get_summaries(user_id):
    if not FIRESTORE_ENABLED or fs is None:
        return jsonify({"status":"error","error":"firestore not enabled"}), 400
    try:
        docs = fs.collection("metabalance_users").document(user_id).collection("summaries").order_by("created_at", direction=firestore.Query.DESCENDING).limit(10).stream()
        out = [d.to_dict() for d in docs]
        return jsonify({"status":"ok","summaries": out})
    except Exception as e:
        return jsonify({"status":"error","error": str(e)}), 500

@app.route("/healthz", methods=["GET"])
def health():
    return jsonify({"status":"ok","time": datetime.utcnow().isoformat()})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting MetaBalance server on port {port} (Firestore enabled: {FIRESTORE_ENABLED}, GCS bucket: {GCS_BUCKET}, AGENT_URL: {AGENT_URL})")
    app.run(host="0.0.0.0", port=port)
