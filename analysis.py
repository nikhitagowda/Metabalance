from datetime import datetime
import csv
import io
from statistics import mean

def parse_csv_text(text):
    """Parse CSV text and return list of dict rows (auto-detect header)."""
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = []
    for r in reader:
        # normalize keys to snake_case used by server
        rows.append({k.strip(): (v.strip() if v is not None else "") for k, v in r.items()})
    return rows

def safe_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except:
        return default

def compute_basic_metrics(rows):
    """Compute summary metrics from parsed rows (list of dicts)."""
    if not rows:
        return {}
    # ensure sorted by date if 'date' exists
    try:
        rows_sorted = sorted(rows, key=lambda r: datetime.fromisoformat(r.get("date")))
    except Exception:
        rows_sorted = rows

    waist_vals = [safe_float(r.get("waist_cm")) for r in rows_sorted if safe_float(r.get("waist_cm")) is not None]
    weight_vals = [safe_float(r.get("weight_kg")) for r in rows_sorted if safe_float(r.get("weight_kg")) is not None]
    steps_vals = [safe_float(r.get("steps")) for r in rows_sorted if safe_float(r.get("steps")) is not None]
    sleep_vals = [safe_float(r.get("sleep_hours")) for r in rows_sorted if safe_float(r.get("sleep_hours")) is not None]
    alcohol_count = sum(1 for r in rows_sorted if r.get("alcohol_intake") and r.get("alcohol_intake").lower() not in ("0","no","none",""))
    cravings_count = sum(1 for r in rows_sorted if r.get("cravings") and r.get("cravings").strip() != "")

    metrics = {
        "rows": len(rows_sorted),
        "avg_waist": round(mean(waist_vals),2) if waist_vals else None,
        "latest_waist": waist_vals[-1] if waist_vals else None,
        "max_waist": max(waist_vals) if waist_vals else None,
        "avg_weight": round(mean(weight_vals),2) if weight_vals else None,
        "latest_weight": weight_vals[-1] if weight_vals else None,
        "avg_steps": int(mean(steps_vals)) if steps_vals else None,
        "avg_sleep": round(mean(sleep_vals),2) if sleep_vals else None,
        "alcohol_days": alcohol_count,
        "cravings_days": cravings_count,
        "height_cm": safe_float(rows_sorted[-1].get("height_cm")) if rows_sorted and rows_sorted[-1].get("height_cm") else None,
        "age": safe_float(rows_sorted[-1].get("age")) if rows_sorted and rows_sorted[-1].get("age") else None,
        "gender": (rows_sorted[-1].get("gender") or "").lower() if rows_sorted else None,
    }
    return metrics

def compute_whtr(latest_waist_cm, height_cm):
    try:
        if not latest_waist_cm or not height_cm:
            return None
        whtr = latest_waist_cm / height_cm
        return round(whtr, 3)
    except Exception:
        return None

def whtr_risk_band(whtr, gender=None):
    if whtr is None:
        return "unknown"
    # simplified thresholds (common clinical heuristics)
    if whtr < 0.5:
        return "healthy"
    if 0.5 <= whtr < 0.6:
        return "elevated"
    return "high"

def compute_metabolic_score(metrics):
    """Simple heuristic score 0-100. Higher is better."""
    score = 50.0
    # steps
    if metrics.get("avg_steps") is not None:
        if metrics["avg_steps"] >= 10000:
            score += 20
        elif metrics["avg_steps"] >= 7000:
            score += 10
        elif metrics["avg_steps"] >= 4000:
            score += 0
        else:
            score -= 10
    # sleep
    if metrics.get("avg_sleep") is not None:
        if metrics["avg_sleep"] >= 7:
            score += 10
        elif metrics["avg_sleep"] >= 6:
            score += 0
        else:
            score -= 10
    # alcohol
    if metrics.get("alcohol_days") is not None:
        if metrics["alcohol_days"] == 0:
            score += 5
        elif metrics["alcohol_days"] >= 3:
            score -= 5
    # cravings
    if metrics.get("cravings_days") is not None and metrics["cravings_days"] > 2:
        score -= 5

    # waist relative penalty
    if metrics.get("avg_waist") is not None and metrics.get("height_cm") is not None:
        whtr = compute_whtr(metrics.get("avg_waist"), metrics.get("height_cm"))
        if whtr:
            if whtr >= 0.6:
                score -= 15
            elif whtr >= 0.5:
                score -= 5

    # clamp 0-100
    score = max(0, min(100, round(score)))
    return score

def metabolic_age_estimate(metabolic_score, chronological_age=None):
    """Rough heuristic: lower score => older metabolic age."""
    if metabolic_score is None:
        return None
    # base metabolic age = chronological age if provided, else 35
    base = chronological_age if chronological_age is not None else 35
    if metabolic_score >= 80:
        return max(16, int(base - 6))
    if metabolic_score >= 60:
        return int(base - 2)
    if metabolic_score >= 40:
        return int(base)
    if metabolic_score >= 20:
        return int(base + 4)
    return int(base + 8)

def female_hormonal_insights(rows, metrics):
    notes = []
    actions = []
    # simple rule: luteal + cravings + poor sleep
    recent = rows[-7:] if len(rows) >= 7 else rows
    luteal_count = sum(1 for r in recent if (r.get("cycle_phase") or "").lower() == "luteal")
    if luteal_count >= 1 and metrics.get("cravings_days",0) >= 1 and (metrics.get("avg_sleep") or 0) < 6.5:
        notes.append("Pattern suggests luteal-phase cravings and low sleep — could indicate PMS/PCOS-like patterns (heuristic).")
        actions.append("Stabilize carbs, add protein at meals, prioritize sleep hygiene for 2 cycles.")
    # weight trend
    if metrics.get("latest_weight") and metrics.get("avg_weight") and metrics["latest_weight"] > metrics["avg_weight"] + 1:
        notes.append("Recent increase in weight vs average—monitor calorie intake and activity.")
        actions.append("Add two 20-min walks daily and reduce late-night snacks.")
    return {"notes": notes, "actions": actions}

def male_hormonal_insights(rows, metrics):
    notes = []
    actions = []
    # low steps + high waist => male metabolic/testosterone risk
    if (metrics.get("avg_steps") or 0) < 5000 and (metrics.get("latest_waist") or 0) >= 95:
        notes.append("Low activity combined with high waist circumference suggests elevated visceral-fat risk and possible testosterone suppression pattern (heuristic).")
        actions.append("Increase daily steps, reduce alcohol, and add morning resistance or HIIT twice weekly.")
    if (metrics.get("avg_sleep") or 0) < 6:
        notes.append("Low sleep can reduce testosterone and recovery.")
        actions.append("Improve sleep routine: consistent bedtime, reduce screens before sleep.")
    return {"notes": notes, "actions": actions}
