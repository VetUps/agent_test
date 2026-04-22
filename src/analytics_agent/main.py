from __future__ import annotations

import argparse
import socket
import threading
import webbrowser
from html import escape

from dash import Dash, Input, Output, dcc, html, dash_table
from flask import Flask, redirect, render_template_string, request, session, url_for

from .analytics import build_figures, build_summary, prepare_materials
from .auth import authenticate, build_demo_users
from .config import get_settings
from .db import DataRepository

LOGIN_TEMPLATE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5efe4;
      --card: #fffdf8;
      --ink: #1d2a36;
      --muted: #607080;
      --accent: #0f6cbd;
      --accent-soft: #d9ecff;
      --warn: #ad3b2e;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Segoe UI", Tahoma, sans-serif;
      background:
        radial-gradient(circle at top right, rgba(15,108,189,.15), transparent 35%),
        linear-gradient(135deg, #f6f3ec 0%, #e9f0f7 100%);
      color: var(--ink);
      display: grid;
      place-items: center;
      padding: 24px;
    }
    .card {
      width: min(460px, 100%);
      background: var(--card);
      border: 1px solid rgba(29,42,54,.08);
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 24px 60px rgba(38,50,56,.12);
    }
    h1 { margin: 0 0 10px; font-size: 30px; }
    p { color: var(--muted); line-height: 1.5; }
    label { display: block; margin-top: 16px; font-weight: 600; }
    input {
      width: 100%;
      margin-top: 8px;
      border-radius: 14px;
      border: 1px solid #d0dae5;
      padding: 14px 16px;
      font-size: 15px;
      background: #fff;
    }
    button {
      width: 100%;
      margin-top: 20px;
      border: 0;
      border-radius: 14px;
      padding: 14px 16px;
      font-size: 15px;
      font-weight: 700;
      background: linear-gradient(135deg, #0f6cbd, #1d8fff);
      color: white;
      cursor: pointer;
    }
    .error {
      margin-top: 14px;
      color: var(--warn);
      font-weight: 600;
    }
    .users {
      margin-top: 20px;
      padding: 14px 16px;
      background: var(--accent-soft);
      border-radius: 16px;
      font-size: 14px;
      line-height: 1.65;
    }
  </style>
</head>
<body>
  <form class="card" method="post">
    <h1>{{ title }}</h1>
    <p>Агент читает данные из <strong>module_b</strong>, при нехватке покрытия автоматически добавляет демонстрационные материалы и строит интерактивный дашборд по модулю Б.</p>
    <label>Логин
      <input name="username" placeholder="viewer / analyst / admin" required>
    </label>
    <label>Пароль
      <input name="password" type="password" placeholder="Введите пароль" required>
    </label>
    <button type="submit">Открыть дашборд</button>
    {% if error %}
      <div class="error">{{ error }}</div>
    {% endif %}
    <div class="users">
      <strong>Демо-доступы:</strong><br>
      `viewer / viewer123` — обзорная роль<br>
      `analyst / analyst123` — полная аналитика<br>
      `admin / admin123` — аналитика + служебная панель
    </div>
  </form>
</body>
</html>
"""


def _find_available_port(host: str, preferred_port: int) -> int:
    with socket.socket() as sock:
        if sock.connect_ex((host, preferred_port)) != 0:
            return preferred_port
    with socket.socket() as sock:
        sock.bind((host, 0))
        return sock.getsockname()[1]


def _summary_card(title: str, value: str, accent: str = "#0f6cbd") -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "13px", "color": "#5d7284", "marginBottom": "8px"}),
            html.Div(value, style={"fontSize": "28px", "fontWeight": "700", "color": accent}),
        ],
        style={
            "background": "white",
            "borderRadius": "22px",
            "padding": "18px 20px",
            "boxShadow": "0 12px 30px rgba(16, 42, 67, 0.08)",
            "border": "1px solid rgba(16, 42, 67, 0.08)",
        },
    )


def _datatable(dataframe, page_size: int = 8):
    return dash_table.DataTable(
        data=dataframe.to_dict("records"),
        columns=[{"name": column, "id": column} for column in dataframe.columns],
        page_size=page_size,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "left",
            "fontFamily": "Segoe UI, Tahoma, sans-serif",
            "padding": "10px",
            "whiteSpace": "normal",
            "height": "auto",
        },
        style_header={"fontWeight": "700", "backgroundColor": "#eef4fb"},
    )


def create_server() -> tuple[Flask, Dash]:
    settings = get_settings()
    server = Flask(__name__)
    server.secret_key = settings.secret_key
    users = build_demo_users()
    repository = DataRepository(settings)

    @server.before_request
    def protect_dash():
        protected_prefixes = ("/dashboard", "/_dash")
        if request.path.startswith(protected_prefixes) and "user" not in session:
            return redirect(url_for("login"))

    @server.route("/")
    def index():
        if session.get("user"):
            return redirect("/dashboard/")
        return redirect(url_for("login"))

    @server.route("/login", methods=["GET", "POST"])
    def login():
        error = ""
        if request.method == "POST":
            username = request.form.get("username", "")
            password = request.form.get("password", "")
            user = authenticate(users, username, password)
            if user:
                session["user"] = user.username
                session["role"] = user.role
                session["display_name"] = user.display_name
                return redirect("/dashboard/")
            error = "Неверный логин или пароль."
        return render_template_string(LOGIN_TEMPLATE, error=error, title=settings.app_title)

    @server.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    dash_app = Dash(
        __name__,
        server=server,
        title=settings.app_title,
        requests_pathname_prefix="/dashboard/",
        routes_pathname_prefix="/dashboard/",
        suppress_callback_exceptions=True,
    )

    def layout():
        role = session.get("role", "viewer")
        display_name = session.get("display_name", "Пользователь")
        return html.Div(
            [
                dcc.Interval(id="refresh-interval", interval=settings.refresh_ms, n_intervals=0),
                dcc.Store(id="session-role", data=role),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div("Конкурсный модуль Б", style={"fontSize": "14px", "letterSpacing": "0.08em", "textTransform": "uppercase", "color": "#6b8192"}),
                                html.H1(settings.app_title, style={"margin": "8px 0 8px", "fontSize": "34px"}),
                                html.Div(
                                    "Dash-агент анализирует учебные материалы из module_b, автоматически закрывает пробелы синтетическими примерами и обновляет витрину по таймеру.",
                                    style={"fontSize": "16px", "maxWidth": "820px", "color": "#42576a"},
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Div(f"Роль: {display_name} ({role})", style={"fontWeight": "700"}),
                                html.A("Выйти", href="/logout", style={"color": "#0f6cbd", "marginTop": "8px", "display": "inline-block"}),
                            ],
                            style={
                                "background": "rgba(255,255,255,0.9)",
                                "padding": "16px 18px",
                                "borderRadius": "18px",
                                "border": "1px solid rgba(15,108,189,.14)",
                            },
                        ),
                    ],
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "minmax(0,1fr) auto",
                        "gap": "18px",
                        "alignItems": "start",
                    },
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("Предмет", style={"fontWeight": "700", "display": "block", "marginBottom": "8px"}),
                                dcc.Dropdown(id="subject-filter", options=[{"label": "Все предметы", "value": "all"}], value="all", clearable=False),
                            ]
                        ),
                        html.Div(
                            [
                                html.Label("Статус данных", style={"fontWeight": "700", "display": "block", "marginBottom": "8px"}),
                                dcc.RadioItems(
                                    id="scope-filter",
                                    options=[
                                        {"label": "Только допущенные", "value": "approved"},
                                        {"label": "Все материалы", "value": "all"},
                                    ],
                                    value="approved",
                                    inline=True,
                                ),
                            ]
                        ),
                    ],
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "minmax(220px, 280px) minmax(280px, 1fr)",
                        "gap": "18px",
                        "marginTop": "22px",
                        "padding": "20px",
                        "background": "rgba(255,255,255,0.88)",
                        "borderRadius": "24px",
                        "border": "1px solid rgba(16,42,67,.08)",
                    },
                ),
                html.Div(id="summary-cards", style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(180px, 1fr))", "gap": "16px", "marginTop": "20px"}),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="coverage-fig"), dcc.Graph(id="generated-share-fig")],
                            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(340px, 1fr))", "gap": "18px"},
                        ),
                        html.Div(
                            [dcc.Graph(id="lesson-distribution-fig"), dcc.Graph(id="categories-fig")],
                            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(340px, 1fr))", "gap": "18px", "marginTop": "18px"},
                        ),
                        html.Div(id="analyst-only-block"),
                        html.Div(id="admin-only-block"),
                    ],
                    style={"marginTop": "18px"},
                ),
            ],
            style={
                "padding": "28px",
                "minHeight": "100vh",
                "background": "linear-gradient(180deg, #f7f2e8 0%, #e9f1f8 100%)",
                "fontFamily": "Segoe UI, Tahoma, sans-serif",
                "color": "#1b2733",
            },
        )

    dash_app.layout = layout

    @dash_app.callback(Output("subject-filter", "options"), Input("refresh-interval", "n_intervals"))
    def load_subject_options(_):
        load = repository.load_dataset()
        options = [{"label": "Все предметы", "value": "all"}]
        options.extend(
            {"label": row["subject_name"], "value": str(row["subject_id"])}
            for row in load.subjects.to_dict("records")
        )
        return options

    @dash_app.callback(
        Output("summary-cards", "children"),
        Output("coverage-fig", "figure"),
        Output("generated-share-fig", "figure"),
        Output("lesson-distribution-fig", "figure"),
        Output("categories-fig", "figure"),
        Output("analyst-only-block", "children"),
        Output("admin-only-block", "children"),
        Input("refresh-interval", "n_intervals"),
        Input("subject-filter", "value"),
        Input("scope-filter", "value"),
        Input("session-role", "data"),
    )
    def refresh_dashboard(_, selected_subject, scope_filter, role):
        load = repository.load_dataset()
        materials = prepare_materials(load, selected_subject or "all", include_rejected=(scope_filter == "all"))
        summary = build_summary(load, materials)
        figures = build_figures(load, materials)

        cards = [
            _summary_card("Материалов в выборке", str(summary["materials_count"])),
            _summary_card("Предметов", str(summary["subjects_count"]), "#00695c"),
            _summary_card("Покрытие тем", f"{summary['coverage_pct']:.1f}%"),
            _summary_card("Средний compliance", f"{summary['avg_compliance_pct']:.1f}%", "#8249b8"),
            _summary_card("Доля автогенерации", f"{summary['generated_share_pct']:.1f}%", "#b85c38"),
            _summary_card("Последняя синхронизация", summary["last_sync_local"], "#34506b"),
        ]

        insights_block = html.Div(
            [
                html.H3("Автоинсайты", style={"margin": "0 0 12px"}),
                html.Ul(
                    [html.Li(text, style={"marginBottom": "8px"}) for text in figures["insights"]] or [html.Li("Инсайты появятся после загрузки данных.")],
                    style={"paddingLeft": "20px", "margin": 0},
                ),
            ],
            style={
                "background": "white",
                "borderRadius": "22px",
                "padding": "18px 20px",
                "marginTop": "18px",
                "boxShadow": "0 12px 30px rgba(16, 42, 67, 0.08)",
            },
        )

        analyst_block = []
        if role in {"analyst", "admin"}:
            analyst_block = html.Div(
                [
                    insights_block,
                    html.Div(
                        [dcc.Graph(figure=figures["deviation_fig"]), dcc.Graph(figure=figures["source_extremes_fig"])],
                        style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(340px, 1fr))", "gap": "18px", "marginTop": "18px"},
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3("TOP требований по предметам", style={"marginTop": 0}),
                                    _datatable(figures["deviation"][["subject_name", "category_label", "subject_avg_pct", "global_avg_pct", "delta_pct"]]),
                                ],
                                style={"background": "white", "borderRadius": "22px", "padding": "18px 20px", "boxShadow": "0 12px 30px rgba(16, 42, 67, 0.08)"},
                            ),
                            html.Div(
                                [
                                    html.H3("Требования: исходные vs сгенерированные", style={"marginTop": 0}),
                                    _datatable(figures["source_extremes"][["source_type_label", "category_label", "avg_score_pct"]]),
                                ],
                                style={"background": "white", "borderRadius": "22px", "padding": "18px 20px", "boxShadow": "0 12px 30px rgba(16, 42, 67, 0.08)"},
                            ),
                        ],
                        style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(320px, 1fr))", "gap": "18px", "marginTop": "18px"},
                    ),
                ]
            )

        admin_block = []
        if role == "admin":
            admin_block = html.Div(
                [
                    html.Div(
                        [
                            html.H3("Служебная панель", style={"marginTop": 0}),
                            html.P(
                                f"Режим дозаполнения данных: {summary['persistence_mode']}. "
                                f"Добавлено синтетических записей за текущую синхронизацию: {summary['generated_rows_added']}.",
                                style={"marginBottom": "14px"},
                            ),
                            _datatable(
                                load.materials[
                                    ["record_id", "subject_name", "title", "resolved_topic_name", "topic_resolution", "dataset_origin", "source_type", "moderation_status"]
                                ].sort_values(["subject_name", "title"]),
                                page_size=10,
                            ),
                        ],
                        style={"background": "white", "borderRadius": "22px", "padding": "18px 20px", "boxShadow": "0 12px 30px rgba(16, 42, 67, 0.08)", "marginTop": "18px"},
                    )
                ]
            )

        return (
            cards,
            figures["coverage_fig"],
            figures["generated_share_fig"],
            figures["lesson_distribution_fig"],
            figures["categories_fig"],
            analyst_block,
            admin_block,
        )

    return server, dash_app


def run(open_browser: bool | None = None, host: str | None = None, port: int | None = None):
    settings = get_settings()
    _, dash_app = create_server()
    host = host or settings.host
    port = _find_available_port(host, port or settings.port)
    should_open_browser = settings.open_browser if open_browser is None else open_browser
    if should_open_browser:
        threading.Timer(1.2, lambda: webbrowser.open(f"http://{host}:{port}/login")).start()
    dash_app.run_server(host=host, port=port, debug=False, dev_tools_hot_reload=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Запуск аналитического агента модуля Б")
    parser.add_argument("--host", default=None, help="Хост для локального сервера Dash")
    parser.add_argument("--port", type=int, default=None, help="Порт для локального сервера Dash")
    parser.add_argument("--no-browser", action="store_true", help="Не открывать браузер автоматически")
    return parser.parse_args()


def main():
    args = parse_args()
    run(open_browser=not args.no_browser, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
