"""Checks for Home Assistant suggestions. No network and no real token."""

import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="lan-watch-test-")
os.environ["HA_URL"] = ""
os.environ["HA_TOKEN"] = ""

import app  # noqa: E402


class HaTests(unittest.TestCase):
    def setUp(self):
        app.HA_FILE.unlink(missing_ok=True)
        app.LIVE_DEVICES.write_text('{"cameras": [{"name": "keep"}], "other": [{"name": "garage-fans", "ip": "192.0.2.10"}]}\n')
        os.environ["HA_URL"] = ""
        os.environ["HA_TOKEN"] = ""

    def test_classify_orders_esp_and_skips_taken(self):
        devices = [
            {
                "name": "Porch light",
                "manufacturer": "espressif",
                "model": "esp32",
                "configuration_url": "http://192.168.2.20:80",
                "identifiers": [["esphome", "aa"]],
            },
            {
                "name": "Speaker",
                "manufacturer": "Sonos",
                "model": "One",
                "configuration_url": "http://192.168.2.21",
                "identifiers": [["sonos", "bb"]],
            },
            {
                "name": "Already",
                "configuration_url": "http://192.168.2.10",
                "identifiers": [["esphome", "cc"]],
            },
            {
                "name": "No address",
                "identifiers": [["esphome", "dd"]],
            },
            {
                "name": "Dashboard",
                "configuration_url": "homeassistant://app/esphome",
                "identifiers": [["esphome", "ee"]],
            },
            {
                "disabled_by": "user",
                "name": "Off",
                "configuration_url": "http://192.168.2.22",
                "identifiers": [["esphome", "ff"]],
            },
        ]
        esp, other = app._classify_ha_devices(devices, {"192.168.2.10"}, {"garage-fans"})
        self.assertEqual([item["name"] for item in esp], ["porch-light"])
        self.assertEqual(esp[0]["ip"], "192.168.2.20")
        self.assertEqual([item["name"] for item in other], ["speaker"])
        self.assertEqual(other[0]["detail"], "One")

    def test_name_collision_gets_a_suffix(self):
        devices = [
            {"name": "Fan", "configuration_url": "http://192.168.2.30", "identifiers": [["esphome", "1"]]},
            {"name": "Fan", "configuration_url": "http://192.168.2.31", "identifiers": [["esphome", "2"]]},
        ]
        esp, other = app._classify_ha_devices(devices, set(), {"fan"})
        self.assertEqual([item["name"] for item in esp], ["fan-2", "fan-3"])
        self.assertEqual(other, [])

    def test_add_many_skips_duplicates_and_keeps_cameras(self):
        items = app._change_device({
            "action": "add_many",
            "devices": [
                {"name": "garage-fans", "ip": "192.0.2.10"},
                {"name": "porch-light", "ip": "192.0.2.20"},
                {"name": "bad", "ip": "not-an-ip"},
                {"name": "speaker", "ip": "192.0.2.21"},
            ],
        })
        names = [item["name"] for item in items]
        self.assertEqual(names, ["garage-fans", "porch-light", "speaker"])
        saved = json.loads(app.LIVE_DEVICES.read_text())
        self.assertEqual(saved["cameras"], [{"name": "keep"}])
        self.assertNotIn("esp", saved)

    def test_save_does_not_return_the_token(self):
        token = "x" * 40
        app._working_ha_url = lambda url: url if url == "http://192.0.2.5:8123" else None
        client = app.app.test_client()
        response = client.post("/api/ha", json={"url": "http://192.0.2.5:8123", "token": token})
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["token_set"])
        self.assertNotIn("token", body)
        self.assertNotIn(token, response.get_data(as_text=True))
        stored = json.loads(app.HA_FILE.read_text())
        self.assertEqual(stored["token"], token)
        self.assertEqual(stat.S_IMODE(app.HA_FILE.stat().st_mode), 0o600)
        blank = client.post("/api/ha", json={"url": "http://192.0.2.5:8123", "token": ""})
        self.assertEqual(blank.status_code, 200)
        self.assertEqual(json.loads(app.HA_FILE.read_text())["token"], token)

    def test_suggestions_without_a_token(self):
        os.environ["HA_URL"] = "http://192.0.2.5:8123"
        app._working_ha_url = lambda url: url
        app._discover_ha = lambda: [{"url": "http://192.0.2.9:8123", "name": "Should not be used"}]
        client = app.app.test_client()
        response = client.get("/api/ha/suggestions")
        body = response.get_json()
        self.assertEqual(response.status_code, 401)
        self.assertEqual(body["url"], "http://192.0.2.5:8123")
        self.assertFalse(body["token_set"])
        self.assertEqual(body["esp"], [])
        self.assertNotIn("token", body)
        os.environ["HA_URL"] = ""

    def test_suggestions_groups_devices(self):
        os.environ["HA_URL"] = "http://192.0.2.5:8123"
        os.environ["HA_TOKEN"] = "y" * 30
        app._working_ha_url = lambda url: url
        app._ha_device_registry = lambda url, token: [
            {
                "name": "Porch light",
                "configuration_url": "http://192.168.2.20",
                "identifiers": [["esphome", "aa"]],
                "model": "esp32",
            }
        ]
        client = app.app.test_client()
        response = client.get("/api/ha/suggestions")
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["esp"][0]["ip"], "192.168.2.20")
        self.assertNotIn(os.environ["HA_TOKEN"], response.get_data(as_text=True))
        os.environ["HA_URL"] = ""
        os.environ["HA_TOKEN"] = ""

    def test_public_addresses_are_left_out(self):
        devices = [
            {"name": "Cloud", "configuration_url": "http://8.8.8.8", "manufacturer": "Example"},
            {"name": "Lamp", "configuration_url": "http://192.168.2.40", "identifiers": [["esphome", "1"]]},
        ]
        esp, other = app._classify_ha_devices(devices, set(), set())
        self.assertEqual([item["name"] for item in esp], ["lamp"])
        self.assertEqual(other, [])

    def test_duplicate_address_is_listed_as_esp(self):
        devices = [
            {"name": "Garage Wall 1", "configuration_url": "http://192.168.2.81", "identifiers": [["wled", "aa"]]},
            {
                "name": "wled board",
                "configuration_url": "http://192.168.2.81",
                "manufacturer": "Espressif",
                "model": "esp32",
            },
        ]
        esp, other = app._classify_ha_devices(devices, set(), set())
        self.assertEqual(other, [])
        self.assertEqual(esp[0]["label"], "Garage Wall 1")
        self.assertEqual(esp[0]["ip"], "192.168.2.81")

    def test_discover_uses_the_saved_address(self):
        os.environ["HA_URL"] = "http://192.0.2.5:8123"
        os.environ["HA_TOKEN"] = "z" * 30
        app._working_ha_url = lambda url: "https://192.0.2.5:8123"
        app._discover_ha = lambda: (_ for _ in ()).throw(AssertionError("network search"))
        response = app.app.test_client().get("/api/ha/discover")
        body = response.get_json()
        self.assertTrue(body["found"])
        self.assertEqual(body["source"], "saved")
        self.assertEqual(body["url"], "https://192.0.2.5:8123")
        self.assertTrue(body["token_set"])
        self.assertNotIn(os.environ["HA_TOKEN"], response.get_data(as_text=True))
        os.environ["HA_URL"] = ""
        os.environ["HA_TOKEN"] = ""

    def test_http_falls_back_to_https(self):
        def fake(url):
            if url.startswith("https://192.0.2.5:8123/"):
                return 200, "<title>Home Assistant</title>"
            return None, ""

        app._http_get = fake
        self.assertEqual(app._working_ha_url("http://192.0.2.5:8123"), "https://192.0.2.5:8123")

    def test_plain_401_still_counts(self):
        def fake(url):
            if url.endswith("/auth/providers"):
                return 200, '{"providers":[{"type":"homeassistant"}]}'
            return 401, "401: Unauthorized"

        app._http_get = fake
        self.assertEqual(app._working_ha_url("https://192.0.2.5:8123"), "https://192.0.2.5:8123")

    def test_redact_and_reject_short_token(self):
        self.assertNotIn("secret-token-value", app._redact("failed secret-token-value", "secret-token-value"))
        with self.assertRaises(ValueError):
            app._save_ha("http://192.0.2.5:8123", "short")


if __name__ == "__main__":
    unittest.main()
