from flask import Flask, request, render_template
from datetime import datetime, timezone
from cf_multi_stats import summarize_handles

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def index():
    results = aggregated = rating_hist = tag_hist = error = None

    if request.method == "POST":
        raw = request.form.get("handles", "")
        handles = [h.strip() for h in raw.split() if h.strip()]

        try:
            start_date = datetime.fromisoformat(
                request.form["start_date"]
            ).replace(tzinfo=timezone.utc)

            end_date = datetime.fromisoformat(
                request.form["end_date"]
            ).replace(tzinfo=timezone.utc)
        except Exception:
            error = "Invalid date range."
            return render_template("index.html", error=error)

        if not handles:
            error = "Please enter at least one handle."
        else:
            results, global_solved = summarize_handles(
                handles, start_date, end_date
            )

            rating_hist = {}
            tag_hist = {}

            for v in global_solved.values():
                rating = v.get("rating")
                tags = v.get("tags", [])

                if rating is not None:
                    rating_hist[rating] = rating_hist.get(rating, 0) + 1

                for t in tags:
                    tag_hist[t] = tag_hist.get(t, 0) + 1

            rated = [v["rating"] for v in global_solved.values() if v["rating"]]

            aggregated = {
                "unique_problems": len(global_solved),
                "avg_rating": sum(rated) / len(rated) if rated else 0.0
            }

    return render_template(
        "index.html",
        results=results,
        aggregated=aggregated,
        rating_hist=rating_hist,
        tag_hist=tag_hist,
    )


if __name__ == "__main__":
    app.run(debug=True)
