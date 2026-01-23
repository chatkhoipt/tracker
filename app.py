import json
from flask import Flask, request, render_template, jsonify
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from cf_multi_stats import _process_handle, _SESSION # Ensure _SESSION is exported in cf_multi_stats.py

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    people_results = []
    error = None

    if request.method == "POST":
        raw_json = request.form.get("people_json", "[]")
        try:
            people_data = json.loads(raw_json)
        except:
            people_data = []
        
        try:
            start_date = datetime.fromisoformat(request.form["start_date"]).replace(tzinfo=timezone.utc)
            end_date = datetime.fromisoformat(request.form["end_date"]).replace(tzinfo=timezone.utc)
            start_ts = int(start_date.timestamp())
            end_ts = int((end_date + timedelta(days=1)).timestamp())
        except:
            error = "Invalid date range."
            return render_template("index.html", error=error)

        all_unique_handles = list(set(h for p in people_data for h in p['handles']))
        handle_data_map = {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_process_handle, h, start_ts, end_ts): h for h in all_unique_handles}
            for future in as_completed(futures):
                h, stats, problems = future.result()
                handle_data_map[h] = {"stats": stats, "problems": problems}

        for person in people_data:
            person_problems = {} 
            person_handles_stats = []
            
            for h in person['handles']:
                data = handle_data_map.get(h)
                if data and data.get('stats') is not None:
                    person_handles_stats.append({"handle": h, "stats": data['stats']})
                    person_problems.update(data['problems']) 
                else:
                    person_handles_stats.append({"handle": h, "stats": None})

            r_hist = {}
            t_hist = {}
            for p_info in person_problems.values():
                r = p_info.get("rating")
                if r: r_hist[r] = r_hist.get(r, 0) + 1
                for t in p_info.get("tags", []): t_hist[t] = t_hist.get(t, 0) + 1

            rated_vals = [p["rating"] for p in person_problems.values() if p["rating"]]
            
            # Convert tuple keys (contestId, index) to strings for JSON
            serializable_problems = {f"{k[0]}_{k[1]}": v for k, v in person_problems.items()}

            people_results.append({
                "name": person['name'],
                "handles": person_handles_stats,
                "total_unique": len(person_problems),
                "avg_rating": sum(rated_vals) / len(rated_vals) if rated_vals else 0,
                "rating_hist": r_hist,
                "tag_hist": t_hist,
                "raw_problems": serializable_problems # Passed for frontend retry updates
            })

    return render_template("index.html", people_results=people_results, error=error)

@app.route("/api/check_handle/<handle>")
def check_handle(handle):
    """Verifies handle existence via CF API."""
    try:
        resp = _SESSION.get(f"https://codeforces.com/api/user.info?handles={handle}", timeout=5)
        return jsonify({"exists": resp.json().get("status") == "OK"})
    except:
        return jsonify({"exists": False})

@app.route("/api/fetch_handle", methods=["POST"])
def fetch_handle():
    """Fetches data for a single handle for the 'Retry' feature."""
    data = request.json
    handle = data.get("handle")
    start_ts = data.get("start_ts")
    end_ts = data.get("end_ts")
    
    h, stats, problems = _process_handle(handle, start_ts, end_ts)
    if stats is None:
        return jsonify({"success": False})
    
    serializable_probs = {f"{k[0]}_{k[1]}": v for k, v in problems.items()}
    return jsonify({"success": True, "stats": stats, "problems": serializable_probs})

if __name__ == "__main__":
    app.run(debug=True)
