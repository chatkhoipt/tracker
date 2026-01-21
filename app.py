from flask import Flask, request, render_template
from cf_multi_stats import summarize_handles, START_DATE

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    results = None
    aggregated = None
    error = None

    if request.method == "POST":
        raw = request.form.get("handles", "")
        handles = [h.strip() for h in raw.split() if h.strip()]

        if not handles:
            error = "Please enter at least one handle."
        else:
            try:
                results, global_solved = summarize_handles(handles)

                rated = [r for r in global_solved.values() if r is not None]
                aggregated = {
                    "unique_problems": len(global_solved),
                    "avg_rating": (
                        sum(rated) / len(rated) if rated else 0.0
                    )
                }
            except Exception as e:
                error = str(e)

    return render_template(
        "index.html",
        results=results,
        aggregated=aggregated,
        start_date=START_DATE.date(),
        error=error
    )


# -------- REQUIRED FOR LOCAL RUN --------
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
