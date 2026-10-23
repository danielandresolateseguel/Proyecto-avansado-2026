import unittest

from app.blueprints.orders import _build_order_creation_event


class OrderCreationEventTests(unittest.TestCase):
    def test_panel_orders_keep_admin_actor(self):
        actor, meta = _build_order_creation_event(
            True,
            actor='caja1',
            customer_name='Cliente',
            customer_phone='123',
        )

        self.assertEqual(actor, 'caja1')
        self.assertEqual(meta['source'], 'panel')
        self.assertEqual(meta['creator_label'], 'caja1')

    def test_online_orders_prefer_customer_name(self):
        actor, meta = _build_order_creation_event(
            False,
            actor='',
            customer_name='Juan Perez',
            customer_phone='123456',
        )

        self.assertEqual(actor, 'Juan Perez')
        self.assertEqual(meta['source'], 'carta_online')
        self.assertEqual(meta['creator_label'], 'Juan Perez')

    def test_online_orders_fallback_to_online_label(self):
        actor, meta = _build_order_creation_event(False)

        self.assertEqual(actor, 'Carta online')
        self.assertEqual(meta['source_label'], 'Carta online')


if __name__ == '__main__':
    unittest.main()
