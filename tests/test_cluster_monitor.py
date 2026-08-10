import unittest

import cluster_monitor as monitor


class ClusterMonitorTests(unittest.TestCase):
    def test_saved_timestamps_use_second_precision(self):
        timestamp = monitor.iso_time(1_786_370_400.987)
        self.assertNotIn(".", timestamp)
        self.assertRegex(timestamp, r"T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$")
        self.assertEqual(monitor.elapsed_second(101.49, 100), 1)
        self.assertEqual(monitor.elapsed_second(101.50, 100), 2)

    def test_default_config_uses_one_second_showuse_sampling(self):
        config = monitor.load_config("monitor_config.jsonc")
        self.assertEqual(config["sample_interval_s"], 1)
        self.assertEqual(config["dcu_utilization_command"], "")
        for key in (
            "dcu_memory_interval_s",
            "dcu_temperature_interval_s",
            "cpu_temperature_interval_s",
            "cpu_power_interval_s",
            "node_power_interval_s",
        ):
            self.assertEqual(config[key], 1)

    def test_business_csv_has_one_timestamp(self):
        self.assertIn("timestamp", monitor.ONE_SECOND_FIELDS)
        self.assertIn("elapsed_s", monitor.ONE_SECOND_FIELDS)
        self.assertNotIn("node_timestamp", monitor.ONE_SECOND_FIELDS)
        self.assertNotIn("received_timestamp", monitor.ONE_SECOND_FIELDS)
        self.assertNotIn("node_clock_offset_s", monitor.ONE_SECOND_FIELDS)

    def test_combined_hy_smi_output_is_parsed(self):
        raw = (
            '{"card0":{"HCU use (%)":"37.5",'
            '"Average Graphics Package Power (W)":"181.0",'
            '"vram Total Memory (MiB)":"100",'
            '"vram Total Used Memory (MiB)":"25",'
            '"Temperature (Sensor junction) (C)":"42.0"}}'
        )
        cards = monitor.parse_dcu_output(raw)
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["util_pct"], 37.5)
        self.assertEqual(cards[0]["power_w"], 181.0)
        self.assertEqual(cards[0]["mem_util_pct"], 25.0)
        self.assertEqual(cards[0]["temp_c"], 42.0)

    def test_showuse_samples_are_fresh_for_steady_state(self):
        rows = []
        for index, utilization in enumerate((10.0, 20.0, 30.0, 40.0)):
            rows.append({
                "elapsed_s": 1.1,
                "dcu_util_sample_age_s": 0.1,
                "dcu_index": index,
                "dcu_util_pct": utilization,
            })
        self.assertEqual(monitor._fresh_node_util_snapshots(rows), [(1.0, 25.0)])

    def test_one_second_wide_row_flattens_all_cards(self):
        sample = {
            "ts": 100.0,
            "cpu_util_pct": 12.5,
            "dcus": [
                {"index": index, "util_pct": index * 10.0, "power_w": 100.0 + index}
                for index in range(4)
            ],
            "ib": [],
        }
        row = monitor.complete_row(sample, "p1c0", "P", 99.0, "unclassified")
        self.assertEqual(row["dcu_count"], 4)
        self.assertEqual(row["dcu0_util_pct"], 0.0)
        self.assertEqual(row["dcu3_util_pct"], 30.0)
        self.assertEqual(row["dcu3_power_w"], 103.0)

if __name__ == "__main__":
    unittest.main()
