from __future__ import annotations

from datetime import datetime
from typing import Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .db import CATEGORY_COLUMNS, RepositoryLoad

LESSON_TYPE_LABELS = {
    "lecture": "Лекция",
    "practice": "Практика",
    "seminar": "Семинар",
    "self_study": "Самостоятельное изучение",
    "lab": "Лабораторная",
}

CATEGORY_LABELS = {
    "compliance_rate": "Общий рейтинг",
    "compliance_structure": "Структура",
    "compliance_length": "Полнота",
    "compliance_clarity": "Понятность",
    "compliance_terminology": "Терминология",
}

SOURCE_LABELS = {
    "original": "Исходные",
    "generated": "Сгенерированные",
}


def prepare_materials(load: RepositoryLoad, selected_subject: str, include_rejected: bool) -> pd.DataFrame:
    materials = load.materials.copy()
    if not include_rejected:
        materials = materials[
            (materials["moderation_status"] == "approved") & (materials["is_allowed"] == 1)
        ]
    if selected_subject != "all":
        materials = materials[materials["subject_id"] == int(selected_subject)]
    materials["lesson_type_label"] = materials["lesson_type"].map(LESSON_TYPE_LABELS).fillna(materials["lesson_type"])
    materials["source_type_label"] = materials["source_type"].map(SOURCE_LABELS).fillna(materials["source_type"])
    return materials


def build_summary(load: RepositoryLoad, materials: pd.DataFrame) -> dict:
    relevant_subject_ids = materials["subject_id"].unique().tolist() if not materials.empty else load.subjects["subject_id"].tolist()
    all_topics = load.topics[load.topics["subject_id"].isin(relevant_subject_ids)]["topic_id"].nunique()
    covered_topics = materials["resolved_topic_id"].nunique()
    avg_compliance = materials["compliance_rate"].mean() or 0
    generated_share = (materials["source_type"] == "generated").mean() if not materials.empty else 0
    return {
        "materials_count": int(len(materials)),
        "subjects_count": int(materials["subject_id"].nunique()),
        "coverage_pct": round(100 * covered_topics / max(all_topics, 1), 1),
        "avg_compliance_pct": float(round(100 * avg_compliance, 1)),
        "generated_share_pct": float(round(100 * generated_share, 1)),
        "generated_rows_added": load.generated_rows_added,
        "last_sync_local": datetime.fromisoformat(load.last_sync_utc).astimezone().strftime("%d.%m.%Y %H:%M:%S"),
        "persistence_mode": load.persistence_mode,
    }


def topic_coverage_df(load: RepositoryLoad, materials: pd.DataFrame) -> pd.DataFrame:
    total_topics = load.topics.merge(
        load.subjects[["subject_id", "subject_name"]],
        on="subject_id",
        how="left",
    )
    total_topics = total_topics.groupby(["subject_id", "subject_name"], as_index=False).size().rename(columns={"size": "total_topics"})
    covered = (
        materials.groupby(["subject_id", "subject_name"], as_index=False)["resolved_topic_id"]
        .nunique()
        .rename(columns={"resolved_topic_id": "covered_topics"})
    )
    result = total_topics.merge(covered, on=["subject_id", "subject_name"], how="left").fillna({"covered_topics": 0})
    result["covered_topics"] = result["covered_topics"].astype(int)
    result["coverage_pct"] = (result["covered_topics"] / result["total_topics"] * 100).round(1)
    overall_avg = round(result["coverage_pct"].mean(), 1) if not result.empty else 0
    result["avg_coverage_pct"] = overall_avg
    result["delta_from_avg"] = (result["coverage_pct"] - overall_avg).round(1)
    return result.sort_values("coverage_pct", ascending=False)


def generated_share_df(materials: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        materials.groupby("subject_name", as_index=False)
        .agg(total_materials=("record_id", "count"), generated_materials=("source_type", lambda s: int((s == "generated").sum())))
    )
    grouped["generated_share_pct"] = (grouped["generated_materials"] / grouped["total_materials"] * 100).round(1)
    overall = pd.DataFrame(
        [
            {
                "subject_name": "Все предметы",
                "total_materials": int(grouped["total_materials"].sum()),
                "generated_materials": int(grouped["generated_materials"].sum()),
            }
        ]
    )
    overall["generated_share_pct"] = (overall["generated_materials"] / overall["total_materials"] * 100).round(1)
    return pd.concat([grouped, overall], ignore_index=True)


def lesson_distribution_df(materials: pd.DataFrame) -> pd.DataFrame:
    subject_dist = (
        materials.groupby(["subject_name", "lesson_type_label"], as_index=False)
        .size()
        .rename(columns={"size": "materials_count"})
    )
    overall = (
        materials.groupby("lesson_type_label", as_index=False)
        .size()
        .rename(columns={"size": "materials_count"})
    )
    overall["subject_name"] = "Все предметы"
    return pd.concat([subject_dist, overall], ignore_index=True)


def compliance_categories_df(materials: pd.DataFrame) -> pd.DataFrame:
    melted = materials.melt(
        id_vars=["subject_name"],
        value_vars=CATEGORY_COLUMNS,
        var_name="category",
        value_name="score",
    )
    melted["category_label"] = melted["category"].map(CATEGORY_LABELS)
    grouped = (
        melted.groupby("category_label", as_index=False)
        .agg(avg_score=("score", "mean"), materials_count=("score", "count"))
    )
    grouped["avg_score_pct"] = (grouped["avg_score"] * 100).round(1)
    grouped["coverage_level"] = pd.cut(
        grouped["avg_score_pct"],
        bins=[0, 70, 85, 100],
        labels=["Риск", "Норма", "Сильная обеспеченность"],
        include_lowest=True,
    ).astype(str)
    return grouped.sort_values("avg_score_pct", ascending=False)


def requirement_deviation_df(materials: pd.DataFrame) -> pd.DataFrame:
    melted = materials.melt(
        id_vars=["subject_name"],
        value_vars=CATEGORY_COLUMNS,
        var_name="category",
        value_name="score",
    )
    melted["category_label"] = melted["category"].map(CATEGORY_LABELS)
    subject_avg = (
        melted.groupby(["subject_name", "category_label"], as_index=False)["score"]
        .mean()
        .rename(columns={"score": "subject_avg"})
    )
    global_avg = (
        melted.groupby("category_label", as_index=False)["score"]
        .mean()
        .rename(columns={"score": "global_avg"})
    )
    result = subject_avg.merge(global_avg, on="category_label", how="left")
    result["delta_pct"] = ((result["subject_avg"] - result["global_avg"]) * 100).round(1)
    result["subject_avg_pct"] = (result["subject_avg"] * 100).round(1)
    result["global_avg_pct"] = (result["global_avg"] * 100).round(1)
    return result.sort_values(["subject_name", "delta_pct"], ascending=[True, False])


def source_extremes_df(materials: pd.DataFrame) -> pd.DataFrame:
    melted = materials.melt(
        id_vars=["source_type_label"],
        value_vars=CATEGORY_COLUMNS,
        var_name="category",
        value_name="score",
    )
    melted["category_label"] = melted["category"].map(CATEGORY_LABELS)
    result = (
        melted.groupby(["source_type_label", "category_label"], as_index=False)["score"]
        .mean()
        .rename(columns={"score": "avg_score"})
    )
    result["avg_score_pct"] = (result["avg_score"] * 100).round(1)
    return result.sort_values(["source_type_label", "avg_score_pct"], ascending=[True, False])


def build_insights(coverage: pd.DataFrame, generated_share: pd.DataFrame, deviation: pd.DataFrame) -> list[str]:
    insights: list[str] = []
    below_avg = coverage[coverage["delta_from_avg"] < 0]
    if not below_avg.empty:
        weakest = below_avg.sort_values("delta_from_avg").iloc[0]
        insights.append(
            f"{weakest['subject_name']}: покрытие тем ниже среднего на {abs(weakest['delta_from_avg']):.1f} п.п."
        )
    top_generated = generated_share[generated_share["subject_name"] != "Все предметы"].sort_values(
        "generated_share_pct", ascending=False
    )
    if not top_generated.empty:
        leader = top_generated.iloc[0]
        insights.append(
            f"{leader['subject_name']}: максимальная доля автогенерации {leader['generated_share_pct']:.1f}%."
        )
    if not deviation.empty:
        weakest_req = deviation.sort_values("delta_pct").iloc[0]
        insights.append(
            f"{weakest_req['subject_name']}: категория '{weakest_req['category_label']}' проседает относительно среднего на "
            f"{abs(weakest_req['delta_pct']):.1f} п.п."
        )
    return insights


def build_figures(load: RepositoryLoad, materials: pd.DataFrame) -> Dict[str, object]:
    coverage = topic_coverage_df(load, materials)
    generated_share = generated_share_df(materials)
    lesson_distribution = lesson_distribution_df(materials)
    categories = compliance_categories_df(materials)
    deviation = requirement_deviation_df(materials)
    source_extremes = source_extremes_df(materials)
    insights = build_insights(coverage, generated_share, deviation)

    coverage_fig = go.Figure()
    coverage_fig.add_bar(
        x=coverage["subject_name"],
        y=coverage["coverage_pct"],
        name="Покрытие тем, %",
        marker_color="#0f6cbd",
    )
    coverage_fig.add_scatter(
        x=coverage["subject_name"],
        y=coverage["avg_coverage_pct"],
        name="Среднее",
        mode="lines+markers",
        line={"color": "#ff7a18", "dash": "dot"},
    )
    coverage_fig.update_layout(
        template="plotly_white",
        title="Покрытие тем по предметам относительно среднего",
        legend={"orientation": "h", "y": 1.12},
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
    )

    generated_share_fig = px.bar(
        generated_share,
        x="subject_name",
        y="generated_share_pct",
        text="generated_share_pct",
        color="subject_name",
        title="Доля автоматически сгенерированных материалов",
        template="plotly_white",
    )
    generated_share_fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    generated_share_fig.update_layout(showlegend=False, margin={"l": 20, "r": 20, "t": 60, "b": 20})

    lesson_distribution_fig = px.bar(
        lesson_distribution,
        x="subject_name",
        y="materials_count",
        color="lesson_type_label",
        barmode="stack",
        title="Распределение материалов по типам занятий",
        template="plotly_white",
    )
    lesson_distribution_fig.update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20})

    categories_fig = px.bar(
        categories,
        x="category_label",
        y="avg_score_pct",
        color="coverage_level",
        text="avg_score_pct",
        title="Обеспеченность требований методических рекомендаций по категориям",
        template="plotly_white",
        color_discrete_map={
            "Сильная обеспеченность": "#009e73",
            "Норма": "#f0ad4e",
            "Риск": "#d9534f",
        },
    )
    categories_fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    categories_fig.update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20})

    deviation_fig = px.bar(
        deviation,
        x="delta_pct",
        y="category_label",
        color="subject_name",
        orientation="h",
        facet_col="subject_name",
        facet_col_wrap=2,
        title="TOP наиболее/наименее выполняемых требований относительно среднего",
        template="plotly_white",
    )
    deviation_fig.update_layout(showlegend=False, margin={"l": 20, "r": 20, "t": 80, "b": 20})
    deviation_fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    source_extremes_fig = px.bar(
        source_extremes,
        x="category_label",
        y="avg_score_pct",
        color="source_type_label",
        barmode="group",
        title="Наиболее и наименее выполняемые требования: исходные vs сгенерированные материалы",
        template="plotly_white",
    )
    source_extremes_fig.update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20})

    return {
        "coverage": coverage,
        "generated_share": generated_share,
        "lesson_distribution": lesson_distribution,
        "categories": categories,
        "deviation": deviation,
        "source_extremes": source_extremes,
        "coverage_fig": coverage_fig,
        "generated_share_fig": generated_share_fig,
        "lesson_distribution_fig": lesson_distribution_fig,
        "categories_fig": categories_fig,
        "deviation_fig": deviation_fig,
        "source_extremes_fig": source_extremes_fig,
        "insights": insights,
    }
