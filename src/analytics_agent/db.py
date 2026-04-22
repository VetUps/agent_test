from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Sequence

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

from .config import Settings

LESSON_TYPES = ["lecture", "practice", "seminar", "self_study", "lab"]
CATEGORY_COLUMNS = [
    "compliance_rate",
    "compliance_structure",
    "compliance_length",
    "compliance_clarity",
    "compliance_terminology",
]


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-zA-Zа-яА-Я0-9]+", " ", value.lower()).strip()


def _tokenize(value: str | None) -> set[str]:
    return {token for token in _normalize_text(value).split() if len(token) > 2}


@dataclass
class RepositoryLoad:
    subjects: pd.DataFrame
    topics: pd.DataFrame
    materials: pd.DataFrame
    generated_rows_added: int
    last_sync_utc: str
    persistence_mode: str


class DataRepository:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._generated_table_ready = False
        self._persistence_mode = "database"

    def connect(self):
        return pymysql.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            database=self.settings.db_name,
            cursorclass=DictCursor,
            autocommit=False,
            charset="utf8mb4",
        )

    def ensure_generated_table(self, connection) -> bool:
        if self._generated_table_ready:
            return True
        sql = """
        CREATE TABLE IF NOT EXISTS analytics_generated_materials (
            generated_material_id INT NOT NULL AUTO_INCREMENT,
            material_code VARCHAR(64) NOT NULL UNIQUE,
            subject_id INT NOT NULL,
            topic_id INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            annotation TEXT NULL,
            full_text_short TEXT NULL,
            lesson_type ENUM('lecture','practice','seminar','self_study','lab') NOT NULL,
            source_type ENUM('generated','original') NOT NULL DEFAULT 'generated',
            moderation_status ENUM('approved','pending','rejected') NOT NULL DEFAULT 'approved',
            is_allowed TINYINT(1) NOT NULL DEFAULT 1,
            compliance_rate DECIMAL(5,3) NULL,
            compliance_structure DECIMAL(5,3) NULL,
            compliance_length DECIMAL(5,3) NULL,
            compliance_clarity DECIMAL(5,3) NULL,
            compliance_terminology DECIMAL(5,3) NULL,
            estimated_duration_min INT NULL,
            has_prev_material TINYINT(1) NOT NULL DEFAULT 0,
            has_next_material TINYINT(1) NOT NULL DEFAULT 0,
            generation_reason VARCHAR(255) NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (generated_material_id),
            KEY idx_agm_subject (subject_id),
            KEY idx_agm_topic (topic_id),
            KEY idx_agm_source (source_type),
            KEY idx_agm_status (moderation_status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
        self._generated_table_ready = True
        self._persistence_mode = "database"
        return True

    def load_dataset(self) -> RepositoryLoad:
        generated_rows_added = 0
        now_utc = datetime.now(timezone.utc).isoformat()
        with self.connect() as connection:
            subjects = self._query_frame(connection, "SELECT * FROM subjects ORDER BY subject_id")
            topics = self._query_frame(
                connection,
                "SELECT * FROM topics WHERE is_active = 1 ORDER BY subject_id, topic_order",
            )

            try:
                self.ensure_generated_table(connection)
            except pymysql.MySQLError:
                connection.rollback()
                self._persistence_mode = "memory"

            base_materials = self._query_frame(connection, "SELECT * FROM materials ORDER BY material_id")
            generated_materials = self._load_generated_materials(connection)

            generated_rows = self._build_synthetic_rows(subjects, topics, base_materials, generated_materials)
            if not generated_rows.empty:
                if self._persistence_mode == "database":
                    generated_rows_added = self._upsert_generated_rows(connection, generated_rows)
                    generated_materials = self._load_generated_materials(connection)
                else:
                    generated_rows_added = len(generated_rows)
                    generated_materials = pd.concat([generated_materials, generated_rows], ignore_index=True)

            materials = self._combine_materials(base_materials, generated_materials, subjects, topics)
            materials = self._resolve_topics(materials, topics)

        return RepositoryLoad(
            subjects=subjects,
            topics=topics,
            materials=materials,
            generated_rows_added=generated_rows_added,
            last_sync_utc=now_utc,
            persistence_mode=self._persistence_mode,
        )

    def _load_generated_materials(self, connection) -> pd.DataFrame:
        try:
            return self._query_frame(
                connection,
                "SELECT * FROM analytics_generated_materials ORDER BY generated_material_id",
            )
        except Exception:
            return pd.DataFrame(
                columns=[
                    "generated_material_id",
                    "material_code",
                    "subject_id",
                    "topic_id",
                    "title",
                    "annotation",
                    "full_text_short",
                    "lesson_type",
                    "source_type",
                    "moderation_status",
                    "is_allowed",
                    *CATEGORY_COLUMNS,
                    "estimated_duration_min",
                    "has_prev_material",
                    "has_next_material",
                    "generation_reason",
                    "created_at",
                    "updated_at",
                ]
            )

    def _query_frame(self, connection, sql: str) -> pd.DataFrame:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        return pd.DataFrame(list(rows))

    def _combine_materials(
        self,
        base_materials: pd.DataFrame,
        generated_materials: pd.DataFrame,
        subjects: pd.DataFrame,
        topics: pd.DataFrame,
    ) -> pd.DataFrame:
        if base_materials.empty:
            base_materials = pd.DataFrame(columns=generated_materials.columns)

        base = base_materials.copy()
        if "material_id" not in base.columns:
            base["material_id"] = range(1, len(base) + 1)
        base["dataset_origin"] = "core"
        base["record_id"] = base["material_id"].astype(str).radd("core-")

        generated = generated_materials.copy()
        if not generated.empty:
            generated["material_id"] = -generated["generated_material_id"].astype(int)
            generated["dataset_origin"] = "synthetic"
            generated["record_id"] = generated["generated_material_id"].astype(str).radd("synthetic-")
        else:
            generated["dataset_origin"] = []
            generated["record_id"] = []

        combined = pd.concat([base, generated], ignore_index=True, sort=False)
        combined["subject_id"] = combined["subject_id"].astype(int)
        combined["topic_id"] = combined["topic_id"].fillna(0).astype(int)
        combined["estimated_duration_min"] = combined["estimated_duration_min"].fillna(0).astype(int)
        combined["is_allowed"] = combined["is_allowed"].fillna(0).astype(int)
        combined["has_prev_material"] = combined["has_prev_material"].fillna(0).astype(int)
        combined["has_next_material"] = combined["has_next_material"].fillna(0).astype(int)
        for column in CATEGORY_COLUMNS:
            combined[column] = pd.to_numeric(combined[column], errors="coerce")

        subject_map = subjects.set_index("subject_id")["subject_name"].to_dict()
        combined["subject_name"] = combined["subject_id"].map(subject_map).fillna("Неизвестный предмет")
        topic_map = topics.set_index("topic_id")["topic_name"].to_dict()
        combined["topic_name_raw"] = combined["topic_id"].map(topic_map)
        return combined

    def _resolve_topics(self, materials: pd.DataFrame, topics: pd.DataFrame) -> pd.DataFrame:
        if materials.empty:
            materials["resolved_topic_id"] = []
            materials["resolved_topic_name"] = []
            materials["topic_resolution"] = []
            return materials

        topics_by_subject: Dict[int, List[dict]] = {}
        for topic in topics.to_dict("records"):
            topics_by_subject.setdefault(int(topic["subject_id"]), []).append(topic)

        resolved_topic_ids: list[int] = []
        resolved_topic_names: list[str] = []
        resolutions: list[str] = []

        for row in materials.to_dict("records"):
            subject_topics = topics_by_subject.get(int(row["subject_id"]), [])
            direct_topic = next((topic for topic in subject_topics if int(topic["topic_id"]) == int(row["topic_id"])), None)
            if direct_topic:
                resolved_topic_ids.append(int(direct_topic["topic_id"]))
                resolved_topic_names.append(direct_topic["topic_name"])
                resolutions.append("direct")
                continue

            text = " ".join(
                [
                    str(row.get("title", "")),
                    str(row.get("annotation", "")),
                    str(row.get("full_text_short", "")),
                ]
            )
            text_tokens = _tokenize(text)
            best_topic = None
            best_score = -1.0
            for topic in subject_topics:
                name = topic["topic_name"]
                score = 0.0
                normalized_name = _normalize_text(name)
                if normalized_name and normalized_name in _normalize_text(text):
                    score += 3.0
                name_tokens = _tokenize(name)
                score += len(text_tokens & name_tokens)
                score += 1 / (1 + abs(int(topic["topic_order"]) - int(row.get("topic_id", 0) or 0)))
                if score > best_score:
                    best_score = score
                    best_topic = topic

            if best_topic:
                resolved_topic_ids.append(int(best_topic["topic_id"]))
                resolved_topic_names.append(best_topic["topic_name"])
                resolutions.append("inferred")
            else:
                resolved_topic_ids.append(int(row["topic_id"]))
                resolved_topic_names.append(row.get("topic_name_raw") or "Не определена")
                resolutions.append("unknown")

        materials["resolved_topic_id"] = resolved_topic_ids
        materials["resolved_topic_name"] = resolved_topic_names
        materials["topic_resolution"] = resolutions
        return materials

    def _build_synthetic_rows(
        self,
        subjects: pd.DataFrame,
        topics: pd.DataFrame,
        base_materials: pd.DataFrame,
        generated_materials: pd.DataFrame,
    ) -> pd.DataFrame:
        existing = pd.concat([base_materials, generated_materials], ignore_index=True, sort=False)
        if existing.empty:
            existing = pd.DataFrame(columns=["subject_id", "topic_id", "source_type", "lesson_type", "moderation_status", "is_allowed"])

        synthetic_rows: list[dict] = []
        subject_lookup = {
            int(row["subject_id"]): row
            for row in subjects.drop_duplicates(subset=["subject_id"]).to_dict("records")
        }

        for topic in topics.to_dict("records"):
            subject_id = int(topic["subject_id"])
            subject_name = subject_lookup[subject_id]["subject_name"]

            subject_existing = existing[existing["subject_id"] == subject_id]
            resolved_topics = set(subject_existing.get("topic_id", pd.Series(dtype="int64")).fillna(0).astype(int).tolist())
            topic_has_material = int(topic["topic_id"]) in resolved_topics
            if topic_has_material and not self._needs_extra_variety(subject_existing):
                continue

            code_suffix = f"{subject_lookup[subject_id]['subject_code']}-{int(topic['topic_order']):02d}"
            title_prefix = "Сгенерированный обзор" if topic_has_material else "Автогенерированный материал"
            lesson_type = LESSON_TYPES[(int(topic["topic_order"]) - 1) % len(LESSON_TYPES)]
            base_value = 0.73 + ((subject_id * 7 + int(topic["topic_order"]) * 3) % 12) / 100
            duration = 24 + int(topic["topic_order"]) * 6
            synthetic_rows.append(
                {
                    "material_code": f"AUTO-{code_suffix}",
                    "subject_id": subject_id,
                    "topic_id": int(topic["topic_id"]),
                    "title": f"{title_prefix}: {topic['topic_name']}",
                    "annotation": (
                        f"Синтетический материал для темы '{topic['topic_name']}' по предмету "
                        f"'{subject_name}', добавленный аналитическим агентом для полноты покрытия."
                    ),
                    "full_text_short": (
                        f"Материал содержит краткое объяснение темы '{topic['topic_name']}', "
                        f"ключевые понятия, примеры и мини-практику для демонстрации в дашборде."
                    ),
                    "lesson_type": lesson_type,
                    "source_type": "generated",
                    "moderation_status": "approved",
                    "is_allowed": 1,
                    "compliance_rate": round(min(base_value + 0.06, 0.95), 3),
                    "compliance_structure": round(min(base_value + 0.03, 0.94), 3),
                    "compliance_length": round(max(base_value - 0.05, 0.68), 3),
                    "compliance_clarity": round(min(base_value + 0.02, 0.94), 3),
                    "compliance_terminology": round(min(base_value + 0.01, 0.93), 3),
                    "estimated_duration_min": duration,
                    "has_prev_material": 1 if int(topic["topic_order"]) > 1 else 0,
                    "has_next_material": 1 if int(topic["topic_order"]) < topics[topics["subject_id"] == subject_id]["topic_order"].max() else 0,
                    "generation_reason": "coverage_gap" if not topic_has_material else "distribution_enrichment",
                }
            )

        if not synthetic_rows:
            return pd.DataFrame()
        return pd.DataFrame(synthetic_rows)

    def _needs_extra_variety(self, subject_existing: pd.DataFrame) -> bool:
        approved = subject_existing[
            (subject_existing.get("moderation_status") == "approved")
            & (subject_existing.get("is_allowed", 0).astype(int) == 1)
        ]
        distinct_types = approved.get("lesson_type", pd.Series(dtype="object")).nunique()
        generated_share = (
            (approved.get("source_type", pd.Series(dtype="object")) == "generated").mean()
            if not approved.empty
            else 0
        )
        return distinct_types < 3 or generated_share < 0.2

    def _upsert_generated_rows(self, connection, generated_rows: pd.DataFrame) -> int:
        if generated_rows.empty:
            return 0
        sql = """
        INSERT INTO analytics_generated_materials (
            material_code, subject_id, topic_id, title, annotation, full_text_short,
            lesson_type, source_type, moderation_status, is_allowed,
            compliance_rate, compliance_structure, compliance_length,
            compliance_clarity, compliance_terminology, estimated_duration_min,
            has_prev_material, has_next_material, generation_reason
        ) VALUES (
            %(material_code)s, %(subject_id)s, %(topic_id)s, %(title)s, %(annotation)s, %(full_text_short)s,
            %(lesson_type)s, %(source_type)s, %(moderation_status)s, %(is_allowed)s,
            %(compliance_rate)s, %(compliance_structure)s, %(compliance_length)s,
            %(compliance_clarity)s, %(compliance_terminology)s, %(estimated_duration_min)s,
            %(has_prev_material)s, %(has_next_material)s, %(generation_reason)s
        )
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            annotation = VALUES(annotation),
            full_text_short = VALUES(full_text_short),
            lesson_type = VALUES(lesson_type),
            moderation_status = VALUES(moderation_status),
            is_allowed = VALUES(is_allowed),
            compliance_rate = VALUES(compliance_rate),
            compliance_structure = VALUES(compliance_structure),
            compliance_length = VALUES(compliance_length),
            compliance_clarity = VALUES(compliance_clarity),
            compliance_terminology = VALUES(compliance_terminology),
            estimated_duration_min = VALUES(estimated_duration_min),
            has_prev_material = VALUES(has_prev_material),
            has_next_material = VALUES(has_next_material),
            generation_reason = VALUES(generation_reason)
        """
        with connection.cursor() as cursor:
            cursor.executemany(sql, generated_rows.to_dict("records"))
        connection.commit()
        return len(generated_rows)
