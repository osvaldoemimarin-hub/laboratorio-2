"""Gráficas exploratorias de la capa Gold (CRISP-DM fase 2)."""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src.config import FIGURES_DIR

COLORES = {"baja": "#9aa5b1", "nominal": "#f0a030", "alta": "#c0392b"}


def generar_graficas(det: pd.DataFrame, figures_dir: Path = FIGURES_DIR) -> None:
    figures_dir.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(7, 8))
    for conf in ["nominal", "baja", "alta"]:
        sub = det[det["confianza"] == conf]
        ax.scatter(sub["longitud"], sub["latitud"], s=1, alpha=0.4, c=COLORES[conf], label=f"{conf} ({len(sub):,})")
    ax.set_xlabel("Longitud"); ax.set_ylabel("Latitud")
    ax.set_title("Detecciones VIIRS NOAA-20 — Sudamérica")
    ax.legend(markerscale=8, loc="lower left")
    ax.set_aspect("equal")
    fig.tight_layout(); fig.savefig(figures_dir / "mapa_detecciones.png", dpi=120); plt.close(fig)

    por_dia = det.groupby(["fecha", "periodo"]).size().unstack(fill_value=0)
    fig, ax = plt.subplots(figsize=(9, 4))
    por_dia.plot.bar(stacked=True, ax=ax, color={"dia": "#f0a030", "noche": "#34495e"})
    ax.set_ylabel("Detecciones"); ax.set_xlabel("")
    ax.set_title("Detecciones por día (UTC)")
    fig.tight_layout(); fig.savefig(figures_dir / "detecciones_por_dia.png", dpi=120); plt.close(fig)

    # El canal I4 satura en 367 K; esos píxeles se muestran como conteo aparte.
    saturado = det["brillo_ti4"] >= 367
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    for conf in ["baja", "nominal", "alta"]:
        sub = det[(det["confianza"] == conf) & ~saturado]
        axes[0].hist(sub["brillo_ti4"], bins=60, alpha=0.6, color=COLORES[conf], label=conf, density=True)
        sub = det[det["confianza"] == conf]
        axes[1].hist(sub["dif_brillo"].clip(-50, 120), bins=60, alpha=0.6, color=COLORES[conf], label=conf, density=True)
    sat = det.assign(saturado=saturado).groupby("confianza")["saturado"].mean().reindex(["baja", "nominal", "alta"])
    axes[2].bar(sat.index, sat.values * 100, color=[COLORES[c] for c in sat.index])
    axes[0].set_title("Brillo I4 (K), píxeles no saturados")
    axes[1].set_title("Diferencia I4 − I5 (K)")
    axes[2].set_title("% de píxeles saturados (I4 = 367 K)")
    axes[0].legend()
    fig.tight_layout(); fig.savefig(figures_dir / "brillo_por_confianza.png", dpi=120); plt.close(fig)
