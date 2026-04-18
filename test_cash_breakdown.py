import unittest

from app.blueprints.cash import _breakdown_total, _normalize_declared_breakdown


class CashBreakdownNormalizationTests(unittest.TestCase):
    def test_uses_transfer_alias_and_preserves_total(self):
        breakdown = _normalize_declared_breakdown(
            {'efectivo': 1200, 'pos': 800, 'trans': 500, 'otros': 100}
        )

        self.assertEqual(
            breakdown,
            {'efectivo': 1200, 'pos': 800, 'transferencia': 500, 'otros': 100},
        )
        self.assertEqual(_breakdown_total(breakdown), 2600)

    def test_falls_back_to_cash_when_breakdown_is_missing(self):
        breakdown = _normalize_declared_breakdown({}, fallback_amount=1750)

        self.assertEqual(
            breakdown,
            {'efectivo': 1750, 'pos': 0, 'transferencia': 0, 'otros': 0},
        )

    def test_reconciles_legacy_breakdown_to_stored_closing_amount(self):
        breakdown = _normalize_declared_breakdown(
            {'pos': 300},
            fallback_amount=1000,
            prefer_fallback=True,
        )

        self.assertEqual(_breakdown_total(breakdown), 1000)
        self.assertEqual(breakdown['pos'], 300)
        self.assertEqual(breakdown['efectivo'], 700)


if __name__ == '__main__':
    unittest.main()
