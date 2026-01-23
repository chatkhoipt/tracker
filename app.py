import json
from flask import Flask, request, render_template
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from cf_multi_stats import _process_handle  # Ensure this matches your file name

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
            # Parse dates and adjust range
            start_date = datetime.fromisoformat(request.form["start_date"]).replace(tzinfo=timezone.utc)
            end_date = datetime.fromisoformat(request.form["end_date"]).replace(tzinfo=timezone.utc)
            start_ts = int(start_date.timestamp())
            end_ts = int((end_date + timedelta(days=1)).timestamp())
        except:
            error = "Invalid date range."
            return render_template("index.html", error=error)

        # 1. Fetch all unique handles in parallel
        all_unique_handles = list(set(h for p in people_data for h in p['handles']))
        handle_data_map = {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_process_handle, h, start_ts, end_ts): h for h in all_unique_handles}
            for future in as_completed(futures):
                h, stats, problems = future.result()
                # If stats is None, 'problems' is an error string
                handle_data_map[h] = {"stats": stats, "problems": problems}

        # 2. Process per Person
        for person in people_data:
            person_problems = {} 
            person_handles_stats = []
            
            for h in person['handles']:
                data = handle_data_map.get(h)
                # FIX: Check if stats exists (successful fetch) before updating
                if data and data.get('stats') is not None:
                    person_handles_stats.append({"handle": h, "stats": data['stats']})
                    person_problems.update(data['problems']) 
                else:
                    # Keep record of failed handle without crashing
                    person_handles_stats.append({"handle": h, "stats": None})

            # Calculate Individual Histograms for the person
            r_hist = {}
            t_hist = {}
            for p_info in person_problems.values():
                r = p_info.get("rating")
                if r: r_hist[r] = r_hist.get(r, 0) + 1
                for t in p_info.get("tags", []): t_hist[t] = t_hist.get(t, 0) + 1

            rated_vals = [p["rating"] for p in person_problems.values() if p["rating"]]
            
            people_results.append({
                "name": person['name'],
                "handles": person_handles_stats,
                "total_unique": len(person_problems),
                "avg_rating": sum(rated_vals) / len(rated_vals) if rated_vals else 0,
                "rating_hist": r_hist,
                "tag_hist": t_hist
            })

    return render_template("index.html", people_results=people_results, error=error)

if __name__ == "__main__":
    app.run(debug=True)
