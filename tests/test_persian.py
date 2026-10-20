from datetime import date, datetime, timezone
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from channel_sync.curator.research import PERSIAN_FORMATTING_RULE, writer_prompt
from channel_sync.curator.topics import questions
from channel_sync.persian import jalali_date, persian_lines, tehran_date


class PersianTests(unittest.TestCase):
    def test_jalali_conversion_at_nowruz_and_leap_day(self):
        cases = {
            "2026-10-20": (1405, 7, 28),
            "2024-03-19": (1402, 12, 29),
            "2024-03-20": (1403, 1, 1),
            "2025-03-20": (1403, 12, 30),
            "2025-03-21": (1404, 1, 1),
            "2026-03-20": (1404, 12, 29),
            "2026-03-21": (1405, 1, 1),
        }
        for gregorian, expected in cases.items():
            with self.subTest(gregorian=gregorian):
                self.assertEqual(jalali_date(date.fromisoformat(gregorian)), expected)

    def test_tehran_date_uses_local_midnight_and_persian_digits(self):
        before = datetime(2026, 10, 19, 20, 29, tzinfo=timezone.utc)
        after = datetime(2026, 10, 19, 20, 30, tzinfo=timezone.utc)
        self.assertEqual(tehran_date(before), "2026-10-19 (۱۴۰۵/۰۷/۲۷)")
        self.assertEqual(tehran_date(after), "2026-10-20 (۱۴۰۵/۰۷/۲۸)")
        with patch("channel_sync.persian.datetime") as clock:
            clock.now.return_value = after
            self.assertEqual(tehran_date(), "2026-10-20 (۱۴۰۵/۰۷/۲۸)")
            clock.now.assert_called_once_with(ZoneInfo("Asia/Tehran"))

    def test_writer_appends_exact_rule_with_a_fresh_date_for_each_call(self):
        with patch(
            "channel_sync.curator.research.tehran_date",
            side_effect=["2026-10-20 (۱۴۰۵/۰۷/۲۸)", "2026-10-21 (۱۴۰۵/۰۷/۲۹)"],
        ):
            first, second = writer_prompt(), writer_prompt()
        self.assertTrue(
            first.endswith(
                PERSIAN_FORMATTING_RULE.replace("{{date}}", "2026-10-20 (۱۴۰۵/۰۷/۲۸)")
            )
        )
        self.assertTrue(second.endswith("Current date: 2026-10-21 (۱۴۰۵/۰۷/۲۹)\n---"))
        self.assertIn('- Right: "کد CSS فعلی فقط جهت متن را درست می‌کند."', first)
        self.assertNotIn("{{date}}", first)

    def test_persian_line_leads_keep_post_exceptions_and_code_blocks(self):
        original = "🧠 OpenAI مدل تازه‌ای معرفی کرد\n• CSS تغییر کرد\n🔗 https://official.example\n#AI\n🆔 @channel\n```python\nprint('hello')\n```"
        result = persian_lines(original, post=True)
        self.assertIn("متن: 🧠 OpenAI", result)
        self.assertIn("متن: • CSS", result)
        self.assertIn("\n🔗 https://official.example\n#AI\n", result)
        self.assertIn("شناسه: 🆔 @channel", result)
        self.assertIn("```python\nprint('hello')\n```", result)
        self.assertEqual(persian_lines(result, post=True), result)

    def test_curator_questions_only_cover_filtering_and_dedup(self):
        self.assertEqual(
            set(questions()),
            {"is_ai_tech_news", "is_useful", "should_curate", "is_same_topic"},
        )
