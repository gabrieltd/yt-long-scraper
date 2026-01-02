"""Streamlit dashboard (read-only) for ranking promising YouTube channels.

ABSOLUTE RULES (enforced by design):
- No yt-dlp, no Playwright, no pipeline execution.
- No metric/scoring recomputation.
- No writes to DB, no schema changes, no table creation.

Data source (only):
- channels_score
- channels_analysis
Join key: channel_url

Run:
  streamlit run dashboard.py

Env:
  - DATABASE_URL or POSTGRES_DSN
  - optional .env (loaded via python-dotenv)
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import asyncpg
import pandas as pd
import streamlit as st
from dotenv import load_dotenv


# Load .env early (but do not require it).
load_dotenv()


def _get_dsn() -> str:
	"""Return Postgres DSN from env.

	Accepted env vars:
	- DATABASE_URL (preferred)
	- POSTGRES_DSN
	"""
	dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
	if not dsn:
		raise RuntimeError(
			"PostgreSQL DSN not configured. Set DATABASE_URL (or POSTGRES_DSN)."
		)
	return dsn


def _run_coro(coro: Any) -> Any:
	"""Run an async coroutine from Streamlit (sync context).

	Streamlit typically runs without an active event loop, but some environments may
	have one. This helper handles both by falling back to a dedicated thread.
	"""
	try:
		return asyncio.run(coro)
	except RuntimeError as exc:
		# If an event loop is already running, run the coroutine in a fresh loop
		# inside a separate thread.
		if "running event loop" not in str(exc):
			raise
		with ThreadPoolExecutor(max_workers=1) as executor:
			future = executor.submit(lambda: asyncio.run(coro))
			return future.result()


async def _fetch_ranking(
	dsn: str,
	*,
	min_score: float,
	max_subs: Optional[int],
	min_long_videos: int,
	limit: int,
) -> pd.DataFrame:
	# SQL parameters are positional ($1..$N) to avoid any SQL injection risk.
	sql = """
	SELECT
		cs.channel_url,
		cs.final_score,
		cs.s_perf,
		cs.s_peak,
		cs.s_consistency,
		cs.s_size,
		ca.subscriber_count,
		ca.median_views_ratio,
		ca.max_views_ratio,
		ca.cycle_long_videos_count,
		ca.max_views,
		ca.analysis_reason
	FROM channels_score cs
	JOIN channels_analysis ca USING (channel_url)
	WHERE cs.final_score >= $1
		AND ($2::bigint IS NULL OR ca.subscriber_count <= $2)
		AND ca.cycle_long_videos_count >= $3
	ORDER BY cs.final_score DESC
	LIMIT $4;
	"""

	conn: asyncpg.Connection | None = None
	try:
		conn = await asyncpg.connect(dsn=dsn, command_timeout=60)
		rows = await conn.fetch(sql, min_score, max_subs, min_long_videos, limit)
		return pd.DataFrame([dict(r) for r in rows])
	finally:
		if conn is not None:
			await conn.close()


async def _fetch_channel_detail(dsn: str, channel_url: str) -> dict[str, Any] | None:
	sql = """
	SELECT
		cs.channel_url,
		cs.final_score,
		cs.s_perf,
		cs.s_peak,
		cs.s_consistency,
		cs.s_size,
		ca.subscriber_count,
		ca.cycle_long_videos_count,
		ca.median_views_ratio,
		ca.max_views_ratio,
		ca.max_views,
		ca.analysis_reason
	FROM channels_score cs
	JOIN channels_analysis ca USING (channel_url)
	WHERE cs.channel_url = $1
	LIMIT 1;
	"""
	conn: asyncpg.Connection | None = None
	try:
		conn = await asyncpg.connect(dsn=dsn, command_timeout=60)
		row = await conn.fetchrow(sql, channel_url)
		return dict(row) if row else None
	finally:
		if conn is not None:
			await conn.close()


def _format_optional(value: Any) -> str:
	if value is None:
		return "—"
	return str(value)


def main() -> None:
	st.set_page_config(page_title="Ranking de Canales (Read-only)", layout="wide")
	st.title("Ranking de canales prometedores")
	st.caption(
		"Herramienta interna (solo lectura). Visualiza resultados ya calculados en Postgres."
	)

	# Sidebar filters -> SQL WHERE (direct translation, no extra business logic).
	with st.sidebar:
		st.header("Filtros")
		min_score = st.slider("Score mínimo", min_value=0.0, max_value=1.0, value=0.0, step=0.01)
		max_subs_input = st.number_input(
			"Subs máximos (0 = sin límite)",
			min_value=0,
			value=0,
			step=1000,
		)
		min_long_videos = st.slider(
			"Mínimo de videos largos en el ciclo",
			min_value=0,
			max_value=200,
			value=0,
			step=1,
		)
		limit = st.number_input("Límite (TOP)", min_value=10, max_value=1000, value=100, step=10)

	max_subs: Optional[int] = None if int(max_subs_input) == 0 else int(max_subs_input)

	# Get DSN and query.
	try:
		dsn = _get_dsn()
	except Exception as exc:
		st.error(
			"No se pudo conectar a Postgres. Revisa DATABASE_URL/POSTGRES_DSN y tu .env."
		)
		st.code(str(exc))
		st.stop()

	with st.spinner("Cargando ranking..."):
		df = _run_coro(
			_fetch_ranking(
				dsn,
				min_score=float(min_score),
				max_subs=max_subs,
				min_long_videos=int(min_long_videos),
				limit=int(limit),
			)
		)

	if df.empty:
		st.info("No hay resultados con los filtros actuales.")
		st.stop()

	# Add rank position based on current ordering (final_score DESC).
	df = df.reset_index(drop=True)
	df.insert(0, "Rank", range(1, len(df) + 1))

	# Main ranking table: exact columns (in this exact order).
	columns_order = [
		"Rank",
		"channel_url",
		"final_score",
		"subscriber_count",
		"median_views_ratio",
		"max_views_ratio",
		"cycle_long_videos_count",
		"max_views",
	]
	df_table = df[columns_order].copy()

	left, right = st.columns([3, 2], gap="large")

	with left:
		st.subheader("Ranking (por final_score)")

		# Streamlit selection is available in newer versions.
		selected_channel_url: Optional[str] = None
		try:
			event = st.dataframe(
				df_table,
				hide_index=True,
				use_container_width=True,
				column_config={
					"channel_url": st.column_config.LinkColumn(
						"channel_url",
						help="Abrir canal",
						display_text=r".*",
					),
				},
				on_select="rerun",
				selection_mode="single-row",
			)
			rows = getattr(getattr(event, "selection", None), "rows", []) if event else []
			if rows:
				selected_channel_url = str(df_table.iloc[int(rows[0])]["channel_url"])
		except TypeError:
			# Older Streamlit: no selection args.
			st.dataframe(
				df_table,
				hide_index=True,
				use_container_width=True,
				column_config={
					"channel_url": st.column_config.LinkColumn(
						"channel_url",
						help="Abrir canal",
						display_text=r".*",
					),
				},
			)

		# Export exactly what is visible (post-filters, post-limit).
		csv_bytes = df_table.to_csv(index=False).encode("utf-8")
		st.download_button(
			"Exportar CSV",
			data=csv_bytes,
			file_name="channels_ranking.csv",
			mime="text/csv",
		)

	with right:
		st.subheader("Detalle del canal")

		# Fallback selector (only used if table selection isn't available or no row selected).
		if selected_channel_url is None:
			selected_channel_url = st.selectbox(
				"Selecciona un canal",
				options=df_table["channel_url"].tolist(),
			)

		with st.spinner("Cargando detalle..."):
			detail = _run_coro(_fetch_channel_detail(dsn, selected_channel_url))

		if not detail:
			st.warning("No se encontró detalle para el canal seleccionado.")
			st.stop()

		# Score total
		st.markdown("**Score total**")
		st.metric("final_score", value=_format_optional(detail.get("final_score")))

		st.markdown("**Desglose de componentes**")
		c1, c2 = st.columns(2)
		with c1:
			st.metric("s_perf", value=_format_optional(detail.get("s_perf")))
			st.metric("s_consistency", value=_format_optional(detail.get("s_consistency")))
		with c2:
			st.metric("s_peak", value=_format_optional(detail.get("s_peak")))
			st.metric("s_size", value=_format_optional(detail.get("s_size")))

		st.markdown("**Métricas del ciclo**")
		st.write(
			{
				"subscriber_count": detail.get("subscriber_count"),
				"cycle_long_videos_count": detail.get("cycle_long_videos_count"),
				"median_views_ratio": detail.get("median_views_ratio"),
				"max_views_ratio": detail.get("max_views_ratio"),
				"max_views": detail.get("max_views"),
			}
		)

		reason = detail.get("analysis_reason")
		if reason:
			st.markdown("**analysis_reason**")
			st.write(reason)


if __name__ == "__main__":
	main()
