"""Modelado (CRISP-DM fases 4 y 5) sobre la capa Gold.

Problema: clasificar el nivel de confianza de cada detección (baja / nominal / alta)
a partir de sus variables radiométricas y de contexto. Se compara un baseline
(regresión logística) con un Random Forest, igual que en el Laboratorio 1.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    ConfusionMatrixDisplay, accuracy_score, classification_report, f1_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.config import FIGURES_DIR, MODELS_DIR, REPORTS_DIR

FEATURES = [
    "brillo_ti4", "brillo_ti5", "dif_brillo", "frp", "scan", "track",
    "es_noche", "hora_utc", "latitud", "longitud",
]
OBJETIVO = "confianza"
CLASES = ["baja", "nominal", "alta"]


def construir_modelos(semilla: int) -> dict[str, Pipeline]:
    return {
        "logistic_regression": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=2000, class_weight="balanced")),
        ]),
        "random_forest": Pipeline([
            ("clf", RandomForestClassifier(
                n_estimators=200, class_weight="balanced", n_jobs=-1, random_state=semilla,
            )),
        ]),
    }


def entrenar_modelos(df: pd.DataFrame, cfg: dict,
                     models_dir: Path = MODELS_DIR, reports_dir: Path = REPORTS_DIR,
                     figures_dir: Path = FIGURES_DIR) -> dict:
    mcfg = cfg["modelo"]
    semilla = mcfg["semilla"]
    for d in (models_dir, reports_dir, figures_dir):
        d.mkdir(parents=True, exist_ok=True)

    X, y = df[FEATURES], df[OBJETIVO]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=mcfg["test_size"], stratify=y, random_state=semilla,
    )
    cv = StratifiedKFold(n_splits=mcfg["cv_folds"], shuffle=True, random_state=semilla)

    resultados, predicciones = {}, {}
    for nombre, modelo in construir_modelos(semilla).items():
        cv_scores = cross_val_score(modelo, X_train, y_train, cv=cv, scoring="f1_macro", n_jobs=1)
        modelo.fit(X_train, y_train)
        pred = modelo.predict(X_test)
        predicciones[nombre] = pred
        resultados[nombre] = {
            "accuracy": round(accuracy_score(y_test, pred), 4),
            "f1_macro": round(f1_score(y_test, pred, average="macro"), 4),
            "cv_f1_macro_media": round(cv_scores.mean(), 4),
            "cv_f1_macro_std": round(cv_scores.std(), 4),
            "reporte": classification_report(y_test, pred, labels=CLASES, output_dict=True, zero_division=0),
        }
        joblib.dump(modelo, models_dir / f"{nombre}_incendios.joblib")

    lr, rf = resultados["logistic_regression"], resultados["random_forest"]
    mejora = rf["f1_macro"] - lr["f1_macro"]
    recomendado = "random_forest" if mejora >= mcfg["margen_minimo_f1"] else "logistic_regression"

    # Matrices de confusión
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for ax, (nombre, pred) in zip(axes, predicciones.items()):
        ConfusionMatrixDisplay.from_predictions(
            y_test, pred, labels=CLASES, ax=ax, colorbar=False, cmap="Oranges",
        )
        ax.set_title(f"{nombre} (F1 macro {resultados[nombre]['f1_macro']:.3f})")
    fig.tight_layout()
    fig.savefig(figures_dir / "matrices_confusion.png", dpi=120)
    plt.close(fig)

    # Importancia de variables (Random Forest)
    rf_model = joblib.load(models_dir / "random_forest_incendios.joblib")
    imp = pd.Series(rf_model.named_steps["clf"].feature_importances_, index=FEATURES).sort_values()
    fig, ax = plt.subplots(figsize=(7, 5))
    imp.plot.barh(ax=ax, color="#d9541e")
    ax.set_title("Importancia de variables — Random Forest")
    fig.tight_layout()
    fig.savefig(figures_dir / "importancia_variables.png", dpi=120)
    plt.close(fig)

    metricas = {
        "fecha_entrenamiento_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_registros": int(len(df)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "distribucion_clases": {k: int(v) for k, v in y.value_counts().items()},
        "features": FEATURES,
        "modelos": resultados,
        "mejora_f1_rf_vs_lr": round(mejora, 4),
        "modelo_recomendado": recomendado,
        "importancia_variables": {k: round(v, 4) for k, v in imp.sort_values(ascending=False).items()},
    }
    (reports_dir / "metricas_modelos.json").write_text(
        json.dumps(metricas, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"LR F1={lr['f1_macro']} | RF F1={rf['f1_macro']} | recomendado: {recomendado}")
    return metricas
