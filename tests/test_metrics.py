import json

from claims.metrics import summarize


def line(rows, rate):
    return json.dumps({"num_input_rows": rows, "processed_rows_per_sec": rate})


def test_summarize_ignores_idle_batches():
    result = summarize([line(0, 0), line(1000, 4000.0), line(0, 0), line(1000, 6000.0)])
    assert result["batches"] == 4 and result["busy_batches"] == 2 and result["total_rows"] == 2000


def test_summarize_rates():
    result = summarize([line(10, 100.0), line(10, 300.0), line(10, 200.0)])
    assert result["processed_rows_per_sec_median"] == 200.0
    assert result["processed_rows_per_sec_max"] == 300.0


def test_summarize_all_idle():
    assert summarize([line(0, 0)]) == {"batches": 1, "busy_batches": 0}


def test_summarize_empty_and_blank_lines():
    assert summarize(["", "  "]) == {"batches": 0, "busy_batches": 0}
