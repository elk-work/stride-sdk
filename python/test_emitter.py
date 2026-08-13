import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from elk_stride import Emitter


class _Ingest(BaseHTTPRequestHandler):
    batches: list[dict] = []
    fail_next = 0

    def do_POST(self):  # noqa: N802
        length = int(self.headers["Content-Length"])
        body = json.loads(self.rfile.read(length))
        if _Ingest.fail_next > 0:
            _Ingest.fail_next -= 1
            self.send_response(500)
            self.end_headers()
            return
        if self.headers.get("Authorization") != "Bearer tok":
            self.send_response(401)
            self.end_headers()
            return
        _Ingest.batches.append(body)
        resp = json.dumps({"accepted": len(body["observations"]), "dropped": 0}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(resp)

    def log_message(self, *args):  # silence
        pass


class EmitterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), _Ingest)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        _Ingest.batches = []
        _Ingest.fail_next = 0

    def emitter(self, **kwargs):
        return Emitter(
            f"http://127.0.0.1:{self.port}",
            "tok",
            {"system": "clawfight", "service": "renderer", "environment": "test"},
            retries=2,
            **kwargs,
        )

    def test_span_success(self):
        e = self.emitter()
        with e.span("clawfight.render.job", request_id="job-1"):
            pass
        e.flush()
        obs = _Ingest.batches[0]["observations"][0]
        self.assertEqual(obs["kind"], "operation_completed")
        self.assertEqual(obs["operation"], "clawfight.render.job")
        self.assertEqual(obs["status"], "success")
        self.assertEqual(obs["context"]["request_id"], "job-1")
        self.assertIn("trace_id", obs["context"])
        self.assertGreaterEqual(obs["duration_ms"], 0)

    def test_span_failure_reraises(self):
        e = self.emitter()
        with self.assertRaises(ValueError):
            with e.span("clawfight.render.job"):
                raise ValueError("boom")
        e.flush()
        obs = _Ingest.batches[0]["observations"][0]
        self.assertEqual(obs["kind"], "operation_failed")
        self.assertEqual(obs["error"]["class"], "ValueError")

    def test_disabled_without_config(self):
        e = Emitter(None, None, {"system": "x", "service": "y", "environment": "z"})
        self.assertFalse(e.enabled)
        e.observe("heartbeat", "hb")
        e.flush()
        self.assertEqual(_Ingest.batches, [])

    def test_never_raises_on_server_error_and_reports_drops(self):
        _Ingest.fail_next = 2  # matches retries: first batch dropped, next succeeds
        e = self.emitter()
        e.observe("heartbeat", "hb")
        e.flush()  # must not raise
        self.assertEqual(_Ingest.batches, [])

        e.observe("heartbeat", "hb2")
        e.flush()
        obs = _Ingest.batches[0]["observations"]
        self.assertEqual(obs[0]["operation"], "watch.sdk.dropped")
        self.assertGreaterEqual(obs[0]["attributes"]["dropped"], 1)
        self.assertEqual(obs[1]["operation"], "hb2")

    def test_auto_flush_at_threshold(self):
        e = self.emitter(flush_at=3)
        for i in range(3):
            e.observe("heartbeat", f"hb.{i}")
        self.assertEqual(len(_Ingest.batches), 1)
        self.assertEqual(len(_Ingest.batches[0]["observations"]), 3)

    def test_attribute_scalars_only(self):
        e = self.emitter()
        e.observe("metric_sampled", "m", attributes={"ok": 1, "bad": {"nested": True}})
        e.flush()
        obs = _Ingest.batches[0]["observations"][0]
        self.assertEqual(obs["attributes"], {"ok": 1})

    def test_unknown_kind_dropped(self):
        e = self.emitter()
        e.observe("not_a_kind", "x")
        e.observe("heartbeat", "hb")
        e.flush()
        obs = _Ingest.batches[0]["observations"]
        self.assertEqual(obs[0]["operation"], "watch.sdk.dropped")
        self.assertEqual(obs[1]["operation"], "hb")

    def test_fixture_shape_parses(self):
        with open("../fixtures/batch.json") as f:
            fixture = json.load(f)
        self.assertEqual(fixture["source"]["system"], "clawfight")
        self.assertEqual(len(fixture["observations"]), 6)


if __name__ == "__main__":
    unittest.main()
