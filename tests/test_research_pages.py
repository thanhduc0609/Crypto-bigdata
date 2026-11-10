"""Render-only routing contract: no DB, training or network required."""
import unittest
from unittest.mock import patch
from web.app import app

class ResearchPagesTests(unittest.TestCase):
    def setUp(self):
        self.client=app.test_client()

    def test_default_and_named_forecast_are_identical_without_market_polling(self):
        with patch('web.app.get_db_connection',side_effect=AssertionError('render must not query DB')):
            default=self.client.get('/'); named=self.client.get('/du-bao-3-nam')
        self.assertEqual(default.status_code,200)
        self.assertEqual(default.data,named.data)
        html=default.get_data(as_text=True)
        self.assertIn('href="/du-bao-3-nam" aria-current="page"',html)
        self.assertIn('id="lt-chart"',html)
        self.assertIn('/static/js/long_term_chart.js',html)
        self.assertNotIn('/static/js/dashboard.js',html)
        self.assertNotIn('id="tv-chart"',html)
        self.assertNotIn('id="ov-price"',html)

    def test_market_has_its_own_url_nav_and_keeps_optional_futures_closed(self):
        response=self.client.get('/thi-truong'); self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        self.assertIn('href="/thi-truong" aria-current="page"',html)
        self.assertIn('/static/js/dashboard.js',html)
        self.assertIn('id="tv-chart"',html)
        self.assertNotIn('id="lt-chart"',html)
        self.assertNotIn('/static/js/long_term.js',html)
        self.assertIn('<details class="hero-subcard futures-subcard">',html)

    def test_shared_system_status_is_collapsed_and_unique_on_both_pages(self):
        for url in ['/','/thi-truong']:
            html=self.client.get(url).get_data(as_text=True)
            self.assertIn('<details class="card system-disclosure">',html)
            self.assertEqual(html.count('id="pipeline-measured"'),1)
            self.assertIn('/static/js/evidence.js',html)

    def test_chart_exposes_presets_accessible_controls_and_uncertainty(self):
        html=self.client.get('/').get_data(as_text=True)
        for value in ['all','five-years','recent-forecast']:
            self.assertIn('data-range="'+value+'"',html)
        for value in ['lt-reset','lt-tooltip','lt-boundary-status','lt-range-status','lt-zoom-in','lt-zoom-out','lt-evaluation','lt-comparison']:
            self.assertIn('id="'+value+'"',html)
        self.assertIn('tabindex="0"',html)
        self.assertIn('id="lt-bands" type="checkbox" checked',html)

if __name__=='__main__': unittest.main()
