# -*- coding: utf-8 -*-
"""Tests for data-quality gate and paper-trade helpers."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import paper_trade
import trade_plan
import trade_guard


class TradeGuardTests(unittest.TestCase):
    def test_evaluate_screen_quality_passes_clean_run(self) -> None:
        universe = [{"code": f"{index:06d}"} for index in range(3000)]
        market_data = {
            "prefilter": {"selected_count": 1800, "seed_quote_count": 2990},
            "source_status": {"quote": True, "kline": True},
            "source_usage": {
                "quote": {"unavailable": 0, "cache_stale": 0, "derived_kline": 0},
                "kline": {"unavailable": 0, "cache_stale": 0},
            },
        }
        screened_data = {
            "report_date": "2026-04-22",
            "fetched_count": 1795,
            "failed_count": 5,
            "qualified_count": 200,
        }

        quality = trade_guard.evaluate_screen_quality(universe, market_data, screened_data, top_n=10)

        self.assertTrue(quality["passed"])
        self.assertEqual(quality["reasons"], [])

    def test_evaluate_screen_quality_ignores_probe_false_negative_when_actual_fetch_succeeds(self) -> None:
        universe = [{"code": f"{index:06d}"} for index in range(3000)]
        market_data = {
            "prefilter": {"selected_count": 1800, "seed_quote_count": 2990},
            "source_status": {"quote": True, "kline": False},
            "source_usage": {
                "quote": {"unavailable": 0, "cache_stale": 0, "derived_kline": 0, "universe_snapshot": 1800},
                "kline": {"unavailable": 0, "cache_stale": 0, "live": 1800},
            },
        }
        screened_data = {
            "report_date": "2026-04-22",
            "fetched_count": 1800,
            "failed_count": 0,
            "qualified_count": 200,
        }

        quality = trade_guard.evaluate_screen_quality(universe, market_data, screened_data, top_n=10)

        self.assertTrue(quality["passed"])

    def test_evaluate_screen_quality_fails_dirty_run_and_writes_report(self) -> None:
        universe = [{"code": f"{index:06d}"} for index in range(100)]
        market_data = {
            "prefilter": {"selected_count": 80, "seed_quote_count": 40},
            "source_status": {"quote": False, "kline": True},
            "source_usage": {
                "quote": {"unavailable": 3, "cache_stale": 2, "derived_kline": 1},
                "kline": {"unavailable": 4, "cache_stale": 1},
            },
        }
        screened_data = {
            "report_date": "2026-04-22",
            "fetched_count": 50,
            "failed_count": 20,
            "qualified_count": 5,
        }

        quality = trade_guard.evaluate_screen_quality(universe, market_data, screened_data, top_n=10)
        self.assertFalse(quality["passed"])
        self.assertTrue(quality["reasons"])

        with tempfile.TemporaryDirectory() as temp_dir:
            report_dir = Path(temp_dir)
            with mock.patch.object(trade_guard, "QUALITY_REPORTS_DIR", report_dir), mock.patch.object(
                trade_guard,
                "ensure_runtime_directories",
                side_effect=lambda: report_dir.mkdir(parents=True, exist_ok=True),
            ):
                report_path = trade_guard.write_screen_quality_report(quality)

            self.assertTrue(report_path.exists())
            text = report_path.read_text(encoding="utf-8")
            self.assertIn("FAIL", text)
            self.assertIn("股票池只有", text)


class PaperTradeTests(unittest.TestCase):
    def test_record_and_review_paper_trade_run(self) -> None:
        screened_data = {
            "report_date": "2026-04-22",
            "suggested_exposure": 1.0,
            "top_stocks": [
                {"code": "000001", "name": "平安银行", "sector": "银行", "price": 10.0, "total_score": 88.0},
                {"code": "600519", "name": "贵州茅台", "sector": "白酒", "price": 20.0, "total_score": 86.0},
            ],
        }

        rows_a = [
            {"date": "2026-04-22", "close": 10.0},
            {"date": "2026-04-23", "close": 10.2},
            {"date": "2026-04-24", "close": 10.5},
        ]
        rows_b = [
            {"date": "2026-04-22", "close": 20.0},
            {"date": "2026-04-23", "close": 19.0},
            {"date": "2026-04-24", "close": 21.0},
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            trade_dir = base / "paper"
            review_dir = trade_dir / "reviews"
            kline_dir = base / "kline"
            trade_dir.mkdir(parents=True, exist_ok=True)
            review_dir.mkdir(parents=True, exist_ok=True)
            kline_dir.mkdir(parents=True, exist_ok=True)
            (kline_dir / "000001.json").write_text(json.dumps({"data": rows_a}, ensure_ascii=False), encoding="utf-8")
            (kline_dir / "600519.json").write_text(json.dumps({"data": rows_b}, ensure_ascii=False), encoding="utf-8")

            with mock.patch.object(paper_trade, "PAPER_TRADES_DIR", trade_dir), mock.patch.object(
                paper_trade, "PAPER_TRADE_REVIEWS_DIR", review_dir
            ), mock.patch.object(paper_trade, "KLINE_CACHE_DIR", kline_dir), mock.patch.object(
                paper_trade,
                "ensure_runtime_directories",
                side_effect=lambda: (
                    trade_dir.mkdir(parents=True, exist_ok=True),
                    review_dir.mkdir(parents=True, exist_ok=True),
                ),
            ):
                log_path = paper_trade.record_paper_trade_run(
                    screened_data,
                    top_n=2,
                    report_path=base / "reports" / "2026-04-22.md",
                    quality_report_path=base / "quality" / "2026-04-22.md",
                    hold_days=2,
                )
                review = paper_trade.review_paper_trade_runs(hold_days=2, as_of_date="2026-04-24")
                review_path = paper_trade.write_paper_trade_review(review)
                self.assertTrue(log_path.exists())
                self.assertEqual(review["run_count"], 1)
                self.assertEqual(review["closed_run_count"], 1)
                self.assertAlmostEqual(review["average_pick_return_pct"], 5.0, places=2)
                self.assertAlmostEqual(review["average_run_return_pct"], 5.0, places=2)
                self.assertTrue(review_path.exists())


class TradePlanTests(unittest.TestCase):
    def test_evaluate_strategy_health_returns_warmup_without_enough_closed_runs(self) -> None:
        review = {
            "as_of_date": "2026-04-22",
            "runs": [
                {"report_date": "2026-04-01", "run_return_pct": 1.2},
                {"report_date": "2026-04-02", "run_return_pct": None},
            ],
        }

        health = trade_plan.evaluate_strategy_health(review)

        self.assertEqual(health["status"], "warmup")
        self.assertEqual(health["deployment_mode"], "paper_only")

    def test_evaluate_strategy_health_can_fail_on_recent_damage(self) -> None:
        review = {
            "as_of_date": "2026-04-22",
            "runs": [
                {"report_date": f"2026-04-{day:02d}", "run_return_pct": value}
                for day, value in enumerate([-2.0, -1.5, -3.0, -2.5, -1.0, -0.8, -1.2, -0.5], start=1)
            ],
        }

        health = trade_plan.evaluate_strategy_health(review)

        self.assertEqual(health["status"], "fail")
        self.assertEqual(health["deployment_mode"], "blocked")

    def test_write_daily_trade_plan_reflects_live_candidate_state(self) -> None:
        screened_data = {
            "report_date": "2026-04-22",
            "suggested_exposure": 0.6,
            "top_stocks": [
                {"code": "000001", "name": "平安银行", "sector": "银行", "price": 10.0, "total_score": 88.0},
                {"code": "600519", "name": "贵州茅台", "sector": "白酒", "price": 20.0, "total_score": 86.0},
            ],
        }
        quality = {"passed": True, "reasons": []}
        health = {
            "status": "pass",
            "deployment_mode": "live_candidate",
            "reasons": ["最近纸面跟踪表现通过健康检查，可以进入实盘候选状态。"],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            plan_dir = Path(temp_dir)
            with mock.patch.object(trade_plan, "TRADE_PLANS_DIR", plan_dir), mock.patch.object(
                trade_plan,
                "ensure_runtime_directories",
                side_effect=lambda: plan_dir.mkdir(parents=True, exist_ok=True),
            ):
                path = trade_plan.write_daily_trade_plan(
                    screened_data,
                    quality=quality,
                    health=health,
                    top_n=2,
                    hold_days=10,
                )

            text = path.read_text(encoding="utf-8")
            self.assertIn("live_candidate", text)
            self.assertIn("30.00%", text)
