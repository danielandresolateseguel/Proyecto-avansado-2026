import sqlite3
import unittest

from app.blueprints.cash import _build_closed_session_display_map


class CashDisplayNumberTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.cur = self.conn.cursor()
        self.cur.execute(
            """
            CREATE TABLE cash_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_slug TEXT NOT NULL,
                closed_at TEXT
            )
            """
        )

    def tearDown(self):
        self.conn.close()

    def test_builds_display_numbers_per_tenant_only_for_closed_sessions(self):
        self.cur.execute("INSERT INTO cash_sessions (tenant_slug, closed_at) VALUES (?, ?)", ('comercio-a', '2026-08-07T10:00:00'))
        first_closed_a = self.cur.lastrowid
        self.cur.execute("INSERT INTO cash_sessions (tenant_slug, closed_at) VALUES (?, ?)", ('comercio-b', '2026-08-07T10:05:00'))
        self.cur.execute("INSERT INTO cash_sessions (tenant_slug, closed_at) VALUES (?, ?)", ('comercio-a', None))
        open_session_a = self.cur.lastrowid
        self.cur.execute("INSERT INTO cash_sessions (tenant_slug, closed_at) VALUES (?, ?)", ('comercio-a', '2026-08-07T11:00:00'))
        second_closed_a = self.cur.lastrowid

        mapping_a = _build_closed_session_display_map(self.cur, 'comercio-a')
        mapping_b = _build_closed_session_display_map(self.cur, 'comercio-b')

        self.assertEqual(mapping_a[first_closed_a], 1)
        self.assertEqual(mapping_a[second_closed_a], 2)
        self.assertNotIn(open_session_a, mapping_a)
        self.assertEqual(mapping_b, {2: 1})


if __name__ == '__main__':
    unittest.main()
