#!/usr/bin/env python3
"""
Load Test Automation Framework
PyQt5-based GUI for creating load test configurations and running tests.
"""

import json
import os
import subprocess
import sys
import threading
from pathlib import Path

from PyQt5.QtCore import (
    Qt, QProcess, pyqtSignal, QObject, QSize, QTimer, QThread,
    QPropertyAnimation, QEasingCurve, QAbstractAnimation, QDateTime,
)
from PyQt5.QtGui import (
    QFont, QIcon, QColor, QPalette, QFontDatabase, QPixmap, QImage,
    QLinearGradient, QPainter,
)
import qtawesome as qta
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QComboBox, QCheckBox, QSpinBox,
    QDoubleSpinBox, QGroupBox, QScrollArea, QTabWidget, QTextEdit,
    QFileDialog, QMessageBox, QSplitter, QFrame, QTableWidget,
    QTableWidgetItem, QHeaderView, QStackedWidget, QListWidget,
    QListWidgetItem, QFormLayout, QSizePolicy, QDialog, QDialogButtonBox,
    QTreeWidget, QTreeWidgetItem, QProgressBar, QToolBar, QAction,
    QAbstractItemView, QStyle, QGraphicsOpacityEffect, QInputDialog,
    QDateTimeEdit,
)

# ---------------------------------------------------------------------------
# Application colour palette  –  deep black, rich crimson accents
# ---------------------------------------------------------------------------
THEME_COLORS = {
    "bg_dark": "#020204",
    "bg_medium": "#08080c",
    "bg_light": "#101018",
    "bg_card": "#050508",
    "bg_elevated": "#0c0c10",
    "red_primary": "#9e3535",
    "red_hover": "#b84a4a",
    "red_dark": "#6b2525",
    "red_glow": "rgba(158, 53, 53, 0.28)",
    "red_subtle": "rgba(158, 53, 53, 0.07)",
    "text_primary": "#d8d8dc",
    "text_secondary": "#808088",
    "text_muted": "#3a3a42",
    "border": "#141418",
    "border_light": "#1e1e26",
    "success": "#1b8a3e",
    "success_glow": "rgba(27, 138, 62, 0.25)",
    "warning": "#d47800",
    "error": "#b84848",
    "error_glow": "rgba(184, 72, 72, 0.22)",
    "input_bg": "#060608",
    "input_border": "#18181e",
    "input_focus": "#c41e1e",
    "table_header": "#060608",
    "table_alt_row": "#040406",
    "scrollbar_bg": "#040406",
    "scrollbar_handle": "#20202a",
    "accent_blue": "#1a3a5c",
    "gradient_start": "#040408",
    "gradient_end": "#08080c",
}

# Serif font stack
_FONT_FAMILY = "'Libre Baskerville', 'Georgia', 'Times New Roman', serif"

STYLESHEET = f"""
/* ── Global ─────────────────────────────────────────────── */
QMainWindow {{
    background-color: {THEME_COLORS['bg_dark']};
}}
QWidget {{
    color: {THEME_COLORS['text_primary']};
    font-family: {_FONT_FAMILY};
    font-size: 15px;
}}

/* ── Labels ─────────────────────────────────────────────── */
QLabel {{
    color: {THEME_COLORS['text_primary']};
    background: transparent;
    font-weight: 400;
    font-size: 15px;
}}
QLabel[class="heading"] {{
    font-size: 24px;
    font-weight: 700;
    color: {THEME_COLORS['red_primary']};
    letter-spacing: 0.5px;
}}
QLabel[class="subheading"] {{
    font-size: 15px;
    font-weight: 700;
    color: {THEME_COLORS['text_secondary']};
    text-transform: uppercase;
    letter-spacing: 1.5px;
}}

/* ── Buttons – pill-shaped, rich glow on hover ──────────── */
QPushButton {{
    background-color: {THEME_COLORS['red_primary']};
    color: #ffffff;
    border: none;
    padding: 10px 26px;
    border-radius: 18px;
    font-weight: 700;
    font-size: 15px;
    min-height: 22px;
    letter-spacing: 0.4px;
}}
QPushButton:hover {{
    background-color: {THEME_COLORS['red_hover']};
    border: 1px solid {THEME_COLORS['red_glow']};
}}
QPushButton:pressed {{
    background-color: {THEME_COLORS['red_dark']};
}}
QPushButton:disabled {{
    background-color: {THEME_COLORS['bg_light']};
    color: {THEME_COLORS['text_muted']};
}}
QPushButton[class="secondary"] {{
    background-color: {THEME_COLORS['bg_light']};
    color: {THEME_COLORS['text_primary']};
    border: 1px solid {THEME_COLORS['border_light']};
}}
QPushButton[class="secondary"]:hover {{
    background-color: {THEME_COLORS['border_light']};
    border-color: {THEME_COLORS['text_muted']};
}}
QPushButton[class="danger"] {{
    background-color: transparent;
    color: {THEME_COLORS['error']};
    border: 1px solid {THEME_COLORS['error']};
}}
QPushButton[class="danger"]:hover {{
    background-color: {THEME_COLORS['error']};
    color: #ffffff;
}}

/* ── Inputs ─────────────────────────────────────────────── */
QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {{
    background-color: {THEME_COLORS['input_bg']};
    color: {THEME_COLORS['text_primary']};
    border: 1px solid {THEME_COLORS['input_border']};
    border-radius: 10px;
    padding: 8px 14px;
    min-height: 22px;
    selection-background-color: {THEME_COLORS['red_primary']};
}}
QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {{
    border: 1px solid {THEME_COLORS['input_focus']};
    background-color: {THEME_COLORS['bg_elevated']};
}}
QComboBox::drop-down {{
    border: none;
    padding-right: 10px;
}}
QComboBox QAbstractItemView {{
    background-color: {THEME_COLORS['bg_elevated']};
    color: {THEME_COLORS['text_primary']};
    selection-background-color: {THEME_COLORS['red_primary']};
    border: 1px solid {THEME_COLORS['border_light']};
    border-radius: 8px;
    padding: 4px;
}}

/* ── Checkboxes ─────────────────────────────────────────── */
QCheckBox {{
    color: {THEME_COLORS['text_primary']};
    spacing: 10px;
    background: transparent;
}}
QCheckBox::indicator {{
    width: 20px;
    height: 20px;
    border: 2px solid {THEME_COLORS['border_light']};
    border-radius: 6px;
    background: {THEME_COLORS['input_bg']};
}}
QCheckBox::indicator:hover {{
    border-color: {THEME_COLORS['red_primary']};
}}
QCheckBox::indicator:checked {{
    background: {THEME_COLORS['red_primary']};
    border-color: {THEME_COLORS['red_primary']};
}}

/* ── Group Boxes ────────────────────────────────────────── */
QGroupBox {{
    background-color: {THEME_COLORS['bg_card']};
    border: 1px solid {THEME_COLORS['border']};
    border-radius: 14px;
    margin-top: 18px;
    padding: 22px;
    padding-top: 34px;
    font-weight: 700;
    font-size: 15px;
}}
QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 5px 16px;
    color: {THEME_COLORS['red_primary']};
    background-color: {THEME_COLORS['bg_card']};
    border: 1px solid {THEME_COLORS['border']};
    border-radius: 8px;
    font-size: 15px;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
}}

/* ── Tabs ───────────────────────────────────────────────── */
QTabWidget::pane {{
    border: 1px solid {THEME_COLORS['border']};
    border-top: none;
    border-radius: 0 0 12px 12px;
    background-color: {THEME_COLORS['bg_dark']};
}}
QTabBar {{
    qproperty-expanding: false;
}}
QTabBar::tab {{
    background-color: {THEME_COLORS['bg_medium']};
    color: {THEME_COLORS['text_muted']};
    padding: 14px 24px;
    border: none;
    border-bottom: 3px solid transparent;
    font-weight: 700;
    font-size: 15px;
    min-width: 200px;
    letter-spacing: 0.8px;
}}
QTabBar::tab:selected {{
    color: {THEME_COLORS['text_primary']};
    border-bottom: 3px solid {THEME_COLORS['red_primary']};
    background-color: {THEME_COLORS['bg_dark']};
}}
QTabBar::tab:hover:!selected {{
    background-color: {THEME_COLORS['bg_light']};
    color: {THEME_COLORS['text_secondary']};
    border-bottom: 3px solid {THEME_COLORS['red_subtle']};
}}

/* ── Console / Text Edit ────────────────────────────────── */
QTextEdit {{
    background-color: {THEME_COLORS['bg_card']};
    color: #f0f0f4;
    border: 1px solid {THEME_COLORS['border']};
    border-radius: 12px;
    padding: 14px;
    font-family: 'Cascadia Code', 'Fira Code', 'JetBrains Mono', 'Consolas', monospace;
    font-size: 15px;
    line-height: 1.6;
}}

/* ── Tables ─────────────────────────────────────────────── */
QTableWidget {{
    background-color: {THEME_COLORS['bg_card']};
    color: {THEME_COLORS['text_primary']};
    border: 1px solid {THEME_COLORS['border']};
    border-radius: 12px;
    gridline-color: {THEME_COLORS['border']};
    selection-background-color: {THEME_COLORS['red_primary']};
    alternate-background-color: {THEME_COLORS['table_alt_row']};
}}
QTableWidget::item {{
    padding: 9px;
    border: none;
}}
QTableWidget::item:hover {{
    background-color: {THEME_COLORS['bg_light']};
}}
QHeaderView::section {{
    background-color: {THEME_COLORS['table_header']};
    color: {THEME_COLORS['text_secondary']};
    padding: 11px 8px;
    border: none;
    border-right: 1px solid {THEME_COLORS['border']};
    border-bottom: 2px solid {THEME_COLORS['red_primary']};
    font-weight: 700;
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 1px;
}}

/* ── List Widget ────────────────────────────────────────── */
QListWidget {{
    background-color: {THEME_COLORS['bg_card']};
    color: {THEME_COLORS['text_primary']};
    border: 1px solid {THEME_COLORS['border']};
    border-radius: 12px;
    outline: none;
    padding: 6px;
}}
QListWidget::item {{
    padding: 12px 18px;
    border-bottom: 1px solid {THEME_COLORS['border']};
    border-radius: 8px;
    margin: 2px 4px;
    font-weight: 600;
}}
QListWidget::item:selected {{
    background-color: {THEME_COLORS['red_primary']};
    color: white;
}}
QListWidget::item:hover:!selected {{
    background-color: {THEME_COLORS['bg_light']};
    border-left: 3px solid {THEME_COLORS['red_primary']};
}}
QListWidget::indicator {{
    width: 22px;
    height: 22px;
    border: 2px solid {THEME_COLORS['border_light']};
    border-radius: 6px;
    background: {THEME_COLORS['input_bg']};
}}
QListWidget::indicator:hover {{
    border-color: {THEME_COLORS['red_primary']};
}}
QListWidget::indicator:checked {{
    background: #2ecc71;
    border-color: #27ae60;
}}

/* ── Scroll Areas & Bars ────────────────────────────────── */
QScrollArea {{
    border: none;
    background: transparent;
}}
QScrollBar:vertical {{
    background: {THEME_COLORS['scrollbar_bg']};
    width: 7px;
    margin: 0;
    border-radius: 3px;
}}
QScrollBar::handle:vertical {{
    background: {THEME_COLORS['scrollbar_handle']};
    min-height: 40px;
    border-radius: 3px;
}}
QScrollBar::handle:vertical:hover {{
    background: {THEME_COLORS['text_muted']};
}}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0;
}}
QScrollBar:horizontal {{
    background: {THEME_COLORS['scrollbar_bg']};
    height: 7px;
    margin: 0;
    border-radius: 3px;
}}
QScrollBar::handle:horizontal {{
    background: {THEME_COLORS['scrollbar_handle']};
    min-width: 40px;
    border-radius: 3px;
}}
QScrollBar::handle:horizontal:hover {{
    background: {THEME_COLORS['text_muted']};
}}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
    width: 0;
}}

/* ── Progress Bar ───────────────────────────────────────── */
QProgressBar {{
    background-color: {THEME_COLORS['bg_medium']};
    border: 1px solid {THEME_COLORS['border']};
    border-radius: 11px;
    text-align: center;
    color: white;
    font-weight: 700;
    min-height: 22px;
}}
QProgressBar::chunk {{
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0,
        stop:0 {THEME_COLORS['red_dark']},
        stop:1 {THEME_COLORS['red_primary']});
    border-radius: 10px;
}}

/* ── Splitter ───────────────────────────────────────────── */
QSplitter::handle {{
    background-color: {THEME_COLORS['border']};
    border-radius: 1px;
}}
QSplitter::handle:horizontal {{
    width: 2px;
}}
QSplitter::handle:vertical {{
    height: 2px;
}}
QSplitter::handle:hover {{
    background-color: {THEME_COLORS['red_primary']};
}}

/* ── Toolbar ────────────────────────────────────────────── */
QToolBar {{
    background-color: {THEME_COLORS['bg_medium']};
    border-bottom: 2px solid {THEME_COLORS['red_primary']};
    padding: 10px 16px;
    spacing: 12px;
}}

/* ── Dialog button box ──────────────────────────────────── */
QDialogButtonBox QPushButton {{
    min-width: 100px;
}}

/* ── Tooltips ───────────────────────────────────────────── */
QToolTip {{
    background-color: {THEME_COLORS['bg_elevated']};
    color: {THEME_COLORS['text_primary']};
    border: 1px solid {THEME_COLORS['border_light']};
    border-radius: 8px;
    padding: 8px 12px;
    font-size: 13px;
}}

/* ── Message Box ────────────────────────────────────────── */
QMessageBox {{
    background-color: {THEME_COLORS['bg_card']};
}}
QMessageBox QLabel {{
    color: {THEME_COLORS['text_primary']};
    font-size: 15px;
}}
"""

# ---------------------------------------------------------------------------
# Protocol definitions (mirrors config_validator.py)
# ---------------------------------------------------------------------------
PROTOCOLS = ["speed_test", "web_browsing", "streaming", "voip_sipp"]

PROTOCOL_PARAMS = {
    "speed_test": {
        "required": {
            "target_url": {"type": "list", "label": "Target URLs (host:port)", "placeholder": "e.g. host:port"},
        },
        "optional": {
            "duration": {"type": "int", "label": "Duration (seconds)", "default": 10, "min": 1},
            "parallel_streams": {"type": "int", "label": "Parallel Streams (-P)", "default": 5, "min": 1, "max": 128},
        },
    },
    "web_browsing": {
        "required": {
            "target_url": {"type": "list", "label": "Target URLs", "placeholder": "e.g. https://www.google.com"},
        },
        "optional": {
            "disable_cache": {"type": "bool", "label": "Disable Cache", "default": True},
        },
    },
    "streaming": {
        "required": {
            "server_url": {"type": "str", "label": "Jellyfin Server URL", "placeholder": "http://host:8096"},
            "api_key": {"type": "str", "label": "API Key", "placeholder": "your-jellyfin-api-key"},
            "item_ids": {"type": "list", "label": "Item IDs", "placeholder": "jellyfin-item-id"},
        },
        "optional": {
            "disable_cache": {"type": "bool", "label": "Disable Cache", "default": True},
            "parallel_browsing": {"type": "bool", "label": "Parallel Browsing", "default": False},
            "aggregate": {"type": "bool", "label": "Aggregate Results", "default": False},
        },
    },
    "voip_sipp": {
        "required": {
            "target_url": {"type": "str", "label": "Target URL (host/IP)", "placeholder": "e.g. 192.168.1.100", "wrap_list": True},
        },
        "optional": {
            "number_of_calls": {"type": "int", "label": "Number of Calls", "default": 5, "min": 1},
            "call_duration": {"type": "int", "label": "Call Duration (seconds)", "default": 5, "min": 1},
            "type": {"type": "choice", "label": "Media Type", "choices": ["none", "audio", "video"], "default": "none"},
            "transport": {"type": "choice", "label": "Transport", "choices": ["udp", "tcp"], "default": "udp"},
        },
    },
}

PROTOCOL_METRICS = {
    "speed_test": ["download_speed", "upload_speed", "jitter", "latency"],
    "web_browsing": ["page_load_time", "ttfb", "dom_content_loaded", "http_response_code", "resource_count", "redirect_count"],
    "streaming": [
        "initial_buffer_time", "test_wall_seconds", "startup_latency_sec",
        "playback_seconds", "active_playback_seconds", "rebuffer_events",
        "rebuffer_ratio", "min_buffer", "max_buffer", "avg_buffer",
        "resolution_switches", "segments_fetched", "non_200_segments",
        "avg_segment_latency_sec", "max_segment_latency_sec", "est_bitrate_bps",
        "error_count", "total_rebuffer_time", "download_speed", "latency", "jitter",
    ],
    "voip_sipp": [
        # SIP signaling (always available)
        "call_success", "call_setup_time", "failed_calls", "retransmissions",
        "timeout_errors", "avg_rtt", "min_rtt", "max_rtt",
        "sip_response_jitter",
        # Audio RTP (type=audio only)
        "audio_rtp_packets", "audio_rtp_packet_loss",
        "audio_rtp_packet_loss_rate", "audio_rtp_jitter",
        "audio_rtp_bitrate_kbps",
        # Video RTP (type=video only)
        "video_rtp_packets", "video_rtp_packet_loss",
        "video_rtp_packet_loss_rate", "video_rtp_jitter",
        "video_rtp_bitrate_kbps",
        # Aggregate media (type=audio or video)
        "jitter", "media_capture_available", "media_streams_observed",
        "media_packets_sent", "media_packets_received",
        "media_bytes_sent", "media_bytes_received",
        "media_packet_loss", "media_packet_loss_rate",
        "media_tx_bitrate_kbps", "media_rx_bitrate_kbps",
    ],
}

# VoIP metrics grouped by media type — used to filter the metric combo
# in ExpectationDialog and to validate expectations in config_validator.
_VOIP_SIGNALING_METRICS = {
    "call_success", "call_setup_time", "failed_calls", "retransmissions",
    "timeout_errors", "avg_rtt", "min_rtt", "max_rtt", "sip_response_jitter",
}
_VOIP_AUDIO_METRICS = {
    "audio_rtp_packets", "audio_rtp_packet_loss",
    "audio_rtp_packet_loss_rate", "audio_rtp_jitter", "audio_rtp_bitrate_kbps",
}
_VOIP_VIDEO_METRICS = {
    "video_rtp_packets", "video_rtp_packet_loss",
    "video_rtp_packet_loss_rate", "video_rtp_jitter", "video_rtp_bitrate_kbps",
}
_VOIP_MEDIA_AGGREGATE_METRICS = {
    "jitter", "media_capture_available", "media_streams_observed",
    "media_packets_sent", "media_packets_received",
    "media_bytes_sent", "media_bytes_received",
    "media_packet_loss", "media_packet_loss_rate",
    "media_tx_bitrate_kbps", "media_rx_bitrate_kbps",
}

VOIP_METRICS_BY_TYPE = {
    "none": _VOIP_SIGNALING_METRICS,
    "audio": _VOIP_SIGNALING_METRICS | _VOIP_AUDIO_METRICS | _VOIP_MEDIA_AGGREGATE_METRICS,
    "video": _VOIP_SIGNALING_METRICS | _VOIP_VIDEO_METRICS | _VOIP_MEDIA_AGGREGATE_METRICS,
}

VALID_OPERATORS = ["lt", "lte", "gt", "gte", "eq", "neq"]
OPERATOR_LABELS = {
    "lt": "< (less than)",
    "lte": "<= (less or equal)",
    "gt": "> (greater than)",
    "gte": ">= (greater or equal)",
    "eq": "== (equal)",
    "neq": "!= (not equal)",
}

VALID_UNITS = [
    "bps", "kbps", "mbps", "gbps", "Bps", "KBps", "MBps", "GBps",
    "ns", "us", "ms", "s", "sec", "seconds", "min", "minutes",
    "count", "code", "ratio",
]

# Mapping from metric category to compatible units (mirrors config_validator logic)
CATEGORY_VALID_UNITS = {
    "speed": ["bps", "kbps", "mbps", "gbps", "Bps", "KBps", "MBps", "GBps"],
    "time": ["ns", "us", "ms", "s", "sec", "seconds", "min", "minutes"],
    "count": ["count", "code", "ratio"],
}

# Metric to category mapping (mirrors unit_converter.METRIC_CATEGORIES)
METRIC_CATEGORY = {
    "download_speed": "speed", "upload_speed": "speed",
    "est_bitrate_bps": "speed", "media_tx_bitrate_kbps": "speed",
    "media_rx_bitrate_kbps": "speed",
    "latency": "time", "jitter": "time", "page_load_time": "time",
    "ttfb": "time", "dom_content_loaded": "time",
    "initial_buffer_time": "time", "test_wall_seconds": "time",
    "startup_latency_sec": "time", "playback_seconds": "time",
    "active_playback_seconds": "time", "min_buffer": "time",
    "max_buffer": "time", "avg_buffer": "time",
    "avg_segment_latency_sec": "time", "max_segment_latency_sec": "time",
    "call_setup_time": "time", "avg_rtt": "time", "min_rtt": "time",
    "max_rtt": "time", "sip_response_jitter": "time",
    "audio_rtp_jitter": "time", "video_rtp_jitter": "time",
    "audio_rtp_packets": "count", "audio_rtp_packet_loss": "count",
    "audio_rtp_packet_loss_rate": "count",
    "audio_rtp_bitrate_kbps": "speed",
    "video_rtp_packets": "count", "video_rtp_packet_loss": "count",
    "video_rtp_packet_loss_rate": "count",
    "video_rtp_bitrate_kbps": "speed",
    "resource_count": "count", "redirect_count": "count",
    "http_response_code": "count", "rebuffer_events": "count",
    "rebuffer_ratio": "count", "resolution_switches": "count",
    "segments_fetched": "count", "non_200_segments": "count",
    "error_count": "count", "call_success": "count",
    "failed_calls": "count", "retransmissions": "count",
    "timeout_errors": "count",
    "media_capture_available": "count", "media_streams_observed": "count",
    "media_packets_sent": "count", "media_packets_received": "count",
    "media_bytes_sent": "count", "media_bytes_received": "count",
    "media_packet_loss": "count", "media_packet_loss_rate": "count",
}

VALID_AGGREGATIONS = ["avg", "min", "max", "stddev"] + [f"p{i}" for i in range(1, 100)]
VALID_SCOPES = ["per_iteration", "scenario"]

# ---------------------------------------------------------------------------
# Human-readable label mapping
# ---------------------------------------------------------------------------
HUMAN_LABELS = {
    # Protocols
    "speed_test": "Speed Test",
    "web_browsing": "Web Browsing",
    "streaming": "Streaming",
    "voip_sipp": "VoIP (SIPp)",
    # Schedule modes
    "once": "Once",
    "recurring": "Recurring",
    # Evaluation scopes
    "per_iteration": "Per Iteration",
    "scenario": "Scenario",
    # Speed test metrics
    "download_speed": "Download Speed",
    "upload_speed": "Upload Speed",
    "jitter": "Jitter",
    "latency": "Latency",
    # Web browsing metrics
    "page_load_time": "Page Load Time",
    "ttfb": "Time to First Byte",
    "dom_content_loaded": "DOM Content Loaded",
    "http_response_code": "HTTP Response Code",
    "resource_count": "Resource Count",
    "redirect_count": "Redirect Count",
    # Streaming metrics
    "initial_buffer_time": "Initial Buffer Time",
    "test_wall_seconds": "Test Wall Seconds",
    "startup_latency_sec": "Startup Latency",
    "playback_seconds": "Playback Seconds",
    "active_playback_seconds": "Active Playback Seconds",
    "rebuffer_events": "Rebuffer Events",
    "rebuffer_ratio": "Rebuffer Ratio",
    "min_buffer": "Min Buffer",
    "max_buffer": "Max Buffer",
    "avg_buffer": "Avg Buffer",
    "resolution_switches": "Resolution Switches",
    "segments_fetched": "Segments Fetched",
    "non_200_segments": "Non-200 Segments",
    "avg_segment_latency_sec": "Avg Segment Latency",
    "max_segment_latency_sec": "Max Segment Latency",
    "est_bitrate_bps": "Est. Bitrate (bps)",
    "error_count": "Error Count",
    # VoIP metrics
    "call_success": "Call Success",
    "call_setup_time": "Call Setup Time",
    "failed_calls": "Failed Calls",
    "retransmissions": "Retransmissions",
    "timeout_errors": "Timeout Errors",
    "avg_rtt": "Avg RTT",
    "min_rtt": "Min RTT",
    "max_rtt": "Max RTT",
    "sip_response_jitter": "SIP Response Jitter",
    "audio_rtp_packets": "Audio RTP Packets",
    "audio_rtp_packet_loss": "Audio RTP Packet Loss",
    "audio_rtp_packet_loss_rate": "Audio RTP Loss Rate",
    "audio_rtp_jitter": "Audio RTP Jitter",
    "audio_rtp_bitrate_kbps": "Audio RTP Bitrate",
    "video_rtp_packets": "Video RTP Packets",
    "video_rtp_packet_loss": "Video RTP Packet Loss",
    "video_rtp_packet_loss_rate": "Video RTP Loss Rate",
    "video_rtp_jitter": "Video RTP Jitter",
    "video_rtp_bitrate_kbps": "Video RTP Bitrate",
    "media_capture_available": "Media Capture Available",
    "media_streams_observed": "Media Streams Observed",
    "media_packets_sent": "Media Packets Sent",
    "media_packets_received": "Media Packets Received",
    "media_bytes_sent": "Media Bytes Sent",
    "media_bytes_received": "Media Bytes Received",
    "media_packet_loss": "Media Packet Loss",
    "media_packet_loss_rate": "Media Packet Loss Rate",
    "media_tx_bitrate_kbps": "Media TX Bitrate",
    "media_rx_bitrate_kbps": "Media RX Bitrate",
    # Aggregations
    "avg": "Average",
    "min": "Minimum",
    "max": "Maximum",
    "stddev": "Std Deviation",
    "percentile": "Percentile",
}


def to_human(key: str) -> str:
    """Convert a snake_case key to a human-readable label."""
    if key in HUMAN_LABELS:
        return HUMAN_LABELS[key]
    return key.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Animation helpers
# ---------------------------------------------------------------------------
def pulse_opacity(widget, duration=1200):
    """Create a subtle pulsing opacity animation (for progress indicators)."""
    effect = QGraphicsOpacityEffect(widget)
    widget.setGraphicsEffect(effect)
    anim = QPropertyAnimation(effect, b"opacity")
    anim.setDuration(duration // 2)
    anim.setStartValue(0.6)
    anim.setEndValue(1.0)
    anim.setEasingCurve(QEasingCurve.InOutSine)
    anim.setLoopCount(-1)  # infinite loop

    def _toggle_direction():
        if anim.direction() == QAbstractAnimation.Forward:
            anim.setDirection(QAbstractAnimation.Backward)
        else:
            anim.setDirection(QAbstractAnimation.Forward)

    anim.finished.connect(_toggle_direction)
    widget._pulse_anim = anim
    return anim


def build_toolbar_logo_pixmap(path, target_height=28):
    """Remove the logo's baked light matte and crop it for dark-toolbar use.

    Uses direct pixel-buffer access instead of per-pixel QImage.pixelColor()
    calls, which avoids ~720 K Python-to-C++ round-trips on typical logos.
    """
    image = QImage(path)
    if image.isNull():
        return QPixmap()

    image = image.convertToFormat(QImage.Format_ARGB32)
    width, height = image.width(), image.height()
    stride = image.bytesPerLine()

    # Access raw pixel data as a mutable buffer.
    # Format_ARGB32 on little-endian: each pixel is [B, G, R, A].
    ptr = image.bits()
    ptr.setsize(height * stride)
    buf = bytearray(ptr)

    bb_min_x, bb_min_y = width, height
    bb_max_x = bb_max_y = -1

    for y in range(height):
        row_off = y * stride
        for x in range(width):
            off = row_off + x * 4
            b, g, r, a = buf[off], buf[off + 1], buf[off + 2], buf[off + 3]

            hi = r if r >= g else g
            if b > hi:
                hi = b
            lo = r if r <= g else g
            if b < lo:
                lo = b
            spread = hi - lo
            brightness = hi

            if spread <= 22 and brightness >= 210:
                if brightness >= 235:
                    a = 0
                else:
                    fade = (235 - brightness) / 25.0
                    a = int(a * (fade if fade < 1.0 else 1.0))
                buf[off + 3] = a

            if a > 0:
                if x < bb_min_x:
                    bb_min_x = x
                if y < bb_min_y:
                    bb_min_y = y
                if x > bb_max_x:
                    bb_max_x = x
                if y > bb_max_y:
                    bb_max_y = y

    # Build a new QImage from the modified buffer and .copy() to own the data.
    result = QImage(bytes(buf), width, height, stride, QImage.Format_ARGB32).copy()

    if bb_max_x < bb_min_x or bb_max_y < bb_min_y:
        return QPixmap.fromImage(result).scaledToHeight(target_height, Qt.SmoothTransformation)

    padding = 6
    bb_min_x = max(0, bb_min_x - padding)
    bb_min_y = max(0, bb_min_y - padding)
    bb_max_x = min(width - 1, bb_max_x + padding)
    bb_max_y = min(height - 1, bb_max_y + padding)
    cropped = result.copy(bb_min_x, bb_min_y, bb_max_x - bb_min_x + 1, bb_max_y - bb_min_y + 1)
    return QPixmap.fromImage(cropped).scaledToHeight(target_height, Qt.SmoothTransformation)


# ---------------------------------------------------------------------------
# Reusable list-input widget — clean read-only list with add/edit/remove
# ---------------------------------------------------------------------------
class ListInputWidget(QWidget):
    """A widget that displays entries as a clean read-only list.

    Each entry is a non-editable row with small edit (pencil) and remove (x)
    icon buttons.  The 'Add' button opens an input dialog; editing an entry
    re-opens the same dialog pre-filled with its current value.
    """

    changed = pyqtSignal()

    # Compact icon-button style shared by edit / remove buttons
    _ICON_BTN_STYLE = (
        f"QPushButton {{ background: transparent; border: none; padding: 2px;"
        f" border-radius: 4px; }}"
        f"QPushButton:hover {{ background: {THEME_COLORS['bg_light']}; }}"
    )

    def __init__(self, placeholder: str = "", add_label: str = "Add Entry",
                 parent: QWidget = None):
        super().__init__(parent)
        self._placeholder = placeholder
        self._add_label = add_label
        self._values: list[str] = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        # --- list container (holds the item rows) ---
        self._list_container = QWidget()
        self._list_container.setStyleSheet(
            f"QWidget#listContainer {{"
            f"  background: {THEME_COLORS['input_bg']};"
            f"  border: 1px solid {THEME_COLORS['input_border']};"
            f"  border-radius: 10px;"
            f"}}"
        )
        self._list_container.setObjectName("listContainer")
        self._list_layout = QVBoxLayout(self._list_container)
        self._list_layout.setContentsMargins(6, 6, 6, 6)
        self._list_layout.setSpacing(0)
        # Sentinel stretch so the items stay top-aligned
        self._list_layout.addStretch()
        layout.addWidget(self._list_container)

        # --- small add button ---
        add_btn = QPushButton(
            qta.icon("fa5s.plus", color=THEME_COLORS["text_secondary"]),
            f" {add_label}",
        )
        add_btn.setCursor(Qt.PointingHandCursor)
        add_btn.setStyleSheet(
            f"QPushButton {{"
            f"  background: {THEME_COLORS['bg_light']};"
            f"  color: {THEME_COLORS['text_secondary']};"
            f"  border: 1px dashed {THEME_COLORS['border_light']};"
            f"  border-radius: 8px;"
            f"  padding: 5px 14px;"
            f"  font-size: 13px;"
            f"  font-weight: 600;"
            f"}}"
            f"QPushButton:hover {{"
            f"  background: {THEME_COLORS['border_light']};"
            f"  color: {THEME_COLORS['text_primary']};"
            f"  border-color: {THEME_COLORS['text_muted']};"
            f"}}"
        )
        add_btn.clicked.connect(self._on_add_clicked)
        layout.addWidget(add_btn, alignment=Qt.AlignLeft)

        # --- row widget tracking ---
        self._row_frames: list[QFrame] = []

    # ------------------------------------------------------------------
    # Public API (unchanged interface for ScenarioEditor)
    # ------------------------------------------------------------------
    def get_values(self) -> list[str]:
        return list(self._values)

    def set_values(self, values: list[str]) -> None:
        # Remove all existing row frames
        for frame in self._row_frames:
            self._list_layout.removeWidget(frame)
            frame.deleteLater()
        self._row_frames.clear()
        self._values.clear()
        for v in values:
            self._append_item(v)
        self._update_container_visibility()

    def add_entry(self, value: str = "") -> None:
        """Programmatic add (keeps back-compat with ScenarioEditor.load_scenario)."""
        if value.strip():
            self._append_item(value.strip())
            self._update_container_visibility()
            self.changed.emit()
        else:
            self._on_add_clicked()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _update_container_visibility(self):
        """Hide the list border when there are no entries."""
        self._list_container.setVisible(len(self._values) > 0)

    def _append_item(self, value: str) -> None:
        """Create a read-only row frame for *value* and add it to the list."""
        idx = len(self._values)
        self._values.append(value)

        frame = QFrame()
        frame.setStyleSheet(
            f"QFrame {{"
            f"  background: transparent;"
            f"  border-bottom: 1px solid {THEME_COLORS['border']};"
            f"  padding: 0px;"
            f"}}"
        )
        row_layout = QHBoxLayout(frame)
        row_layout.setContentsMargins(10, 6, 4, 6)
        row_layout.setSpacing(6)

        # Bullet / index indicator
        bullet = QLabel("\u2022")
        bullet.setFixedWidth(14)
        bullet.setStyleSheet(
            f"color: {THEME_COLORS['red_primary']}; font-size: 16px;"
            f" background: transparent; border: none;"
        )
        row_layout.addWidget(bullet)

        # Read-only value label
        label = QLabel(value)
        label.setStyleSheet(
            f"color: {THEME_COLORS['text_primary']}; font-size: 14px;"
            f" background: transparent; border: none;"
        )
        label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        label.setWordWrap(True)
        row_layout.addWidget(label, 1)

        # Edit button (pencil icon)
        edit_btn = QPushButton(qta.icon("fa5s.pen", color=THEME_COLORS["text_muted"]), "")
        edit_btn.setFixedSize(24, 24)
        edit_btn.setCursor(Qt.PointingHandCursor)
        edit_btn.setToolTip("Edit")
        edit_btn.setStyleSheet(self._ICON_BTN_STYLE)
        edit_btn.clicked.connect(lambda checked=False, f=frame: self._on_edit_clicked(f))
        row_layout.addWidget(edit_btn)

        # Remove button (x icon)
        remove_btn = QPushButton(qta.icon("fa5s.times", color=THEME_COLORS["error"]), "")
        remove_btn.setFixedSize(24, 24)
        remove_btn.setCursor(Qt.PointingHandCursor)
        remove_btn.setToolTip("Remove")
        remove_btn.setStyleSheet(self._ICON_BTN_STYLE)
        remove_btn.clicked.connect(lambda checked=False, f=frame: self._on_remove_clicked(f))
        row_layout.addWidget(remove_btn)

        # Insert before the trailing stretch
        insert_pos = self._list_layout.count() - 1
        self._list_layout.insertWidget(insert_pos, frame)
        self._row_frames.append(frame)

    def _frame_index(self, frame: QFrame) -> int:
        """Return the logical index of *frame*, or -1."""
        try:
            return self._row_frames.index(frame)
        except ValueError:
            return -1

    def _on_add_clicked(self):
        hint = f"  (e.g. {self._placeholder})" if self._placeholder else ""
        text, ok = QInputDialog.getText(
            self, self._add_label, f"Enter value:{hint}",
            QLineEdit.Normal, "",
        )
        if ok and text.strip():
            self._append_item(text.strip())
            self._update_container_visibility()
            self.changed.emit()

    def _on_edit_clicked(self, frame: QFrame):
        idx = self._frame_index(frame)
        if idx < 0:
            return
        current = self._values[idx]
        hint = f"  (e.g. {self._placeholder})" if self._placeholder else ""
        text, ok = QInputDialog.getText(
            self, "Edit Entry", f"Value:{hint}",
            QLineEdit.Normal, current,
        )
        if ok and text.strip():
            self._values[idx] = text.strip()
            # Update the label inside the frame (second widget in the row layout)
            label = frame.layout().itemAt(1).widget()
            if isinstance(label, QLabel):
                label.setText(text.strip())
            self.changed.emit()

    def _on_remove_clicked(self, frame: QFrame):
        idx = self._frame_index(frame)
        if idx < 0:
            return
        self._values.pop(idx)
        self._row_frames.pop(idx)
        self._list_layout.removeWidget(frame)
        frame.deleteLater()
        self._update_container_visibility()
        self.changed.emit()


# ---------------------------------------------------------------------------
# Signals helper
# ---------------------------------------------------------------------------
class ProcessSignals(QObject):
    output = pyqtSignal(str)
    finished = pyqtSignal(int)


# ---------------------------------------------------------------------------
# Expectation Editor Dialog
# ---------------------------------------------------------------------------
class ExpectationDialog(QDialog):
    """Dialog for adding/editing a single expectation."""

    def __init__(self, protocol, expectation=None, media_type=None, parent=None):
        super().__init__(parent)
        self.protocol = protocol
        self.media_type = media_type
        self.setWindowTitle("Edit Expectation" if expectation else "Add Expectation")
        self.setMinimumWidth(450)
        self.setStyleSheet(STYLESHEET)
        self._build_ui(expectation)

    def _build_ui(self, exp):
        layout = QFormLayout(self)
        layout.setSpacing(12)

        metrics = PROTOCOL_METRICS.get(self.protocol, [])
        # For voip_sipp, filter metrics based on the scenario's media type
        if self.protocol == "voip_sipp" and self.media_type is not None:
            allowed = VOIP_METRICS_BY_TYPE.get(self.media_type)
            if allowed is not None:
                metrics = [m for m in metrics if m in allowed]
        self.metric_combo = QComboBox()
        for m in metrics:
            self.metric_combo.addItem(to_human(m), m)
        layout.addRow("Metric:", self.metric_combo)

        self.operator_combo = QComboBox()
        for op in VALID_OPERATORS:
            self.operator_combo.addItem(OPERATOR_LABELS[op], op)
        layout.addRow("Operator:", self.operator_combo)

        self.value_spin = QDoubleSpinBox()
        self.value_spin.setRange(-999999, 999999)
        self.value_spin.setDecimals(4)
        layout.addRow("Value:", self.value_spin)

        self.unit_combo = QComboBox()
        layout.addRow("Unit:", self.unit_combo)

        self.agg_combo = QComboBox()
        for agg in ["avg", "min", "max", "stddev", "percentile"]:
            self.agg_combo.addItem(to_human(agg), agg)
        self.agg_combo.currentIndexChanged.connect(self._on_aggregation_changed)
        self._percentile_value = None  # stores the chosen p1-p99 value
        layout.addRow("Aggregation:", self.agg_combo)

        self.scope_combo = QComboBox()
        for scope in VALID_SCOPES:
            self.scope_combo.addItem(to_human(scope), scope)
        layout.addRow("Evaluation Scope:", self.scope_combo)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

        # Wire metric selection to filter compatible units
        self.metric_combo.currentIndexChanged.connect(lambda: self._update_units(self.metric_combo.currentData()))
        self._update_units(self.metric_combo.currentData())

        # Populate if editing
        if exp:
            idx = self.metric_combo.findData(exp.get("metric", ""))
            if idx >= 0:
                self.metric_combo.setCurrentIndex(idx)
            idx = self.operator_combo.findData(exp.get("operator", ""))
            if idx >= 0:
                self.operator_combo.setCurrentIndex(idx)
            self.value_spin.setValue(exp.get("value", 0))
            idx = self.unit_combo.findText(exp.get("unit", ""))
            if idx >= 0:
                self.unit_combo.setCurrentIndex(idx)
            agg = exp.get("aggregation", "")
            if agg.startswith("p") and agg[1:].isdigit():
                self._percentile_value = int(agg[1:])
                idx = self.agg_combo.findData("percentile")
                if idx >= 0:
                    self.agg_combo.setCurrentIndex(idx)
            else:
                idx = self.agg_combo.findData(agg)
                if idx >= 0:
                    self.agg_combo.setCurrentIndex(idx)
            idx = self.scope_combo.findData(exp.get("evaluation_scope", ""))
            if idx >= 0:
                self.scope_combo.setCurrentIndex(idx)

    def _update_units(self, metric):
        """Filter the unit combo to show only units compatible with the selected metric."""
        category = METRIC_CATEGORY.get(metric, "count")
        compatible = CATEGORY_VALID_UNITS.get(category, VALID_UNITS)
        prev_unit = self.unit_combo.currentText()
        self.unit_combo.blockSignals(True)
        self.unit_combo.clear()
        self.unit_combo.addItems(compatible)
        # Restore previous selection if still compatible
        idx = self.unit_combo.findText(prev_unit)
        if idx >= 0:
            self.unit_combo.setCurrentIndex(idx)
        self.unit_combo.blockSignals(False)

    def _on_aggregation_changed(self, index):
        """Prompt for percentile value when 'percentile' is selected."""
        if self.agg_combo.currentData() == "percentile":
            val, ok = QInputDialog.getInt(
                self, "Percentile", "Enter percentile (1-99):",
                value=self._percentile_value or 50, min=1, max=99,
            )
            if ok:
                self._percentile_value = val
            else:
                # User cancelled — revert to avg
                self.agg_combo.blockSignals(True)
                avg_idx = self.agg_combo.findData("avg")
                if avg_idx >= 0:
                    self.agg_combo.setCurrentIndex(avg_idx)
                self.agg_combo.blockSignals(False)
                self._percentile_value = None

    def get_expectation(self):
        agg = self.agg_combo.currentData()
        if agg == "percentile" and self._percentile_value is not None:
            agg = f"p{self._percentile_value}"
        return {
            "metric": self.metric_combo.currentData(),
            "operator": self.operator_combo.currentData(),
            "value": self.value_spin.value(),
            "unit": self.unit_combo.currentText(),
            "aggregation": agg,
            "evaluation_scope": self.scope_combo.currentData(),
        }


# ---------------------------------------------------------------------------
# Scenario Editor Widget
# ---------------------------------------------------------------------------
class ScenarioEditor(QWidget):
    """Editor for a single scenario."""

    changed = pyqtSignal()

    def __init__(self, scenario=None, parent=None):
        super().__init__(parent)
        self._building = True
        self._build_ui()
        if scenario:
            self.load_scenario(scenario)
        self._building = False

    def _emit_changed(self):
        if not self._building:
            self.changed.emit()

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        splitter = QSplitter(Qt.Vertical)

        # ── Top: config groups in a scrollable area ──
        top_scroll = QScrollArea()
        top_scroll.setWidgetResizable(True)
        top_scroll.setFrameShape(QFrame.NoFrame)
        top_container = QWidget()
        top_layout = QVBoxLayout(top_container)
        top_layout.setSpacing(12)

        # -- Basic Info & Schedule side-by-side --
        top_row = QHBoxLayout()

        basic_group = QGroupBox("Basic Information")
        basic_layout = QFormLayout()
        basic_layout.setSpacing(10)

        self.id_edit = QLineEdit()
        self.id_edit.setPlaceholderText("unique_scenario_id")
        self.id_edit.textChanged.connect(self._emit_changed)
        basic_layout.addRow("Scenario ID:", self.id_edit)

        self.desc_edit = QLineEdit()
        self.desc_edit.setPlaceholderText("Human-readable description")
        self.desc_edit.textChanged.connect(self._emit_changed)
        basic_layout.addRow("Description:", self.desc_edit)

        self.protocol_combo = QComboBox()
        for p in PROTOCOLS:
            self.protocol_combo.addItem(to_human(p), p)
        self.protocol_combo.currentIndexChanged.connect(lambda: self._on_protocol_changed(self.protocol_combo.currentData()))
        basic_layout.addRow("Protocol:", self.protocol_combo)

        basic_group.setLayout(basic_layout)
        top_row.addWidget(basic_group)

        sched_group = QGroupBox("Schedule")
        sched_layout = QFormLayout()
        sched_layout.setSpacing(10)

        self.mode_combo = QComboBox()
        for m in ["once", "recurring"]:
            self.mode_combo.addItem(to_human(m), m)
        self.mode_combo.currentIndexChanged.connect(lambda: self._on_mode_changed(self.mode_combo.currentData()))
        sched_layout.addRow("Mode:", self.mode_combo)

        self.start_time_combo = QComboBox()
        self.start_time_combo.addItem("Immediate", "immediate")
        self.start_time_combo.addItem("ISO Datetime (UTC)", "iso")
        self.start_time_combo.currentIndexChanged.connect(self._on_start_time_mode_changed)
        sched_layout.addRow("Start Time:", self.start_time_combo)

        self.start_time_input = QLineEdit()
        self.start_time_input.setPlaceholderText("e.g. 2025-06-15T14:30:00Z")
        self.start_time_input.textChanged.connect(self._emit_changed)
        self.start_time_input_label = QLabel("Datetime (UTC):")
        self.start_time_input.setVisible(False)
        self.start_time_input_label.setVisible(False)
        sched_layout.addRow(self.start_time_input_label, self.start_time_input)

        self.interval_spin = QDoubleSpinBox()
        self.interval_spin.setRange(0.01, 99999)
        self.interval_spin.setValue(1)
        self.interval_spin.valueChanged.connect(self._emit_changed)
        self.interval_label = QLabel("Interval (min):")
        sched_layout.addRow(self.interval_label, self.interval_spin)

        self.duration_spin = QDoubleSpinBox()
        self.duration_spin.setRange(0.001, 99999)
        self.duration_spin.setValue(1)
        self.duration_spin.setDecimals(3)
        self.duration_spin.valueChanged.connect(self._emit_changed)
        self.duration_label = QLabel("Duration (hrs):")
        sched_layout.addRow(self.duration_label, self.duration_spin)

        sched_group.setLayout(sched_layout)
        top_row.addWidget(sched_group)

        top_layout.addLayout(top_row)

        # -- Parameters (dynamic based on protocol) --
        self.params_group = QGroupBox("Parameters")
        self.params_layout = QFormLayout()
        self.params_layout.setSpacing(10)
        self.params_group.setLayout(self.params_layout)
        top_layout.addWidget(self.params_group)
        self.param_widgets = {}

        top_layout.addStretch()
        top_scroll.setWidget(top_container)
        splitter.addWidget(top_scroll)

        # ── Bottom: Expectations with full space ──
        exp_widget = QWidget()
        exp_outer = QVBoxLayout(exp_widget)
        exp_outer.setContentsMargins(0, 0, 0, 0)
        exp_outer.setSpacing(8)

        exp_group = QGroupBox("Expectations")
        exp_layout = QVBoxLayout()
        exp_layout.setSpacing(8)

        self.exp_table = QTableWidget()
        self.exp_table.setColumnCount(6)
        self.exp_table.setHorizontalHeaderLabels(["Metric", "Operator", "Value", "Unit", "Aggregation", "Scope"])
        self.exp_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.exp_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.exp_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.exp_table.setAlternatingRowColors(True)
        self.exp_table.verticalHeader().setVisible(False)
        self.exp_table.setMinimumHeight(160)
        exp_layout.addWidget(self.exp_table, 1)

        btn_row = QHBoxLayout()
        add_exp_btn = QPushButton(qta.icon("fa5s.plus-circle", color=THEME_COLORS['text_primary']), " Add Expectation")
        add_exp_btn.clicked.connect(self._add_expectation)
        btn_row.addWidget(add_exp_btn)

        edit_exp_btn = QPushButton(qta.icon("fa5s.edit", color=THEME_COLORS['text_primary']), " Edit")
        edit_exp_btn.setProperty("class", "secondary")
        edit_exp_btn.clicked.connect(self._edit_expectation)
        btn_row.addWidget(edit_exp_btn)

        del_exp_btn = QPushButton(qta.icon("fa5s.minus-circle", color=THEME_COLORS['error']), " Remove")
        del_exp_btn.setProperty("class", "danger")
        del_exp_btn.clicked.connect(self._remove_expectation)
        btn_row.addWidget(del_exp_btn)

        btn_row.addStretch()
        exp_layout.addLayout(btn_row)
        exp_group.setLayout(exp_layout)
        exp_outer.addWidget(exp_group)
        splitter.addWidget(exp_widget)

        # Give expectations ~45% of vertical space
        splitter.setSizes([550, 450])
        splitter.setChildrenCollapsible(False)
        main_layout.addWidget(splitter)

        # Initialize
        self._on_mode_changed(self.mode_combo.currentData() or "once")
        self._on_protocol_changed(self.protocol_combo.currentData() or "speed_test")

        # Store expectations data
        self._expectations = []

    def _on_start_time_mode_changed(self):
        is_iso = self.start_time_combo.currentData() == "iso"
        self.start_time_input.setVisible(is_iso)
        self.start_time_input_label.setVisible(is_iso)
        self._emit_changed()

    def _on_mode_changed(self, mode):
        is_recurring = (mode == "recurring")
        self.interval_spin.setVisible(is_recurring)
        self.interval_label.setVisible(is_recurring)
        self.duration_spin.setVisible(is_recurring)
        self.duration_label.setVisible(is_recurring)
        self._emit_changed()

    def _on_protocol_changed(self, protocol):
        # Clear old param widgets
        while self.params_layout.count():
            item = self.params_layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()
        self.param_widgets.clear()

        proto_def = PROTOCOL_PARAMS.get(protocol, {})

        # Required params
        for key, info in proto_def.get("required", {}).items():
            self._add_param_widget(key, info)

        # Optional params
        for key, info in proto_def.get("optional", {}).items():
            self._add_param_widget(key, info)

        self._emit_changed()

    def _add_param_widget(self, key, info):
        ptype = info["type"]
        label = info.get("label", key)

        if ptype == "list":
            add_label = f"Add {label.split('(')[0].strip()}"
            w = ListInputWidget(
                placeholder=info.get("placeholder", ""),
                add_label=add_label,
            )
            w.changed.connect(self._emit_changed)
        elif ptype == "str":
            w = QLineEdit()
            w.setPlaceholderText(info.get("placeholder", ""))
            w.textChanged.connect(self._emit_changed)
        elif ptype == "int":
            w = QSpinBox()
            w.setRange(info.get("min", 0), info.get("max", 99999))
            w.setValue(info.get("default", 1))
            w.valueChanged.connect(self._emit_changed)
        elif ptype == "bool":
            w = QCheckBox()
            w.setChecked(info.get("default", False))
            w.stateChanged.connect(self._emit_changed)
        elif ptype == "choice":
            w = QComboBox()
            w.addItems(info.get("choices", []))
            default = info.get("default", "")
            idx = w.findText(default)
            if idx >= 0:
                w.setCurrentIndex(idx)
            w.currentTextChanged.connect(self._emit_changed)
        else:
            w = QLineEdit()
            w.textChanged.connect(self._emit_changed)

        self.param_widgets[key] = (info, w)
        self.params_layout.addRow(f"{label}:", w)

    def _get_voip_media_type(self):
        """Return the current voip_sipp media type parameter, or None."""
        if self.protocol_combo.currentData() != "voip_sipp":
            return None
        info_widget = self.param_widgets.get("type")
        if info_widget is None:
            return None
        _, w = info_widget
        if isinstance(w, QComboBox):
            return w.currentText()
        return None

    def _add_expectation(self):
        protocol = self.protocol_combo.currentData()
        dlg = ExpectationDialog(protocol, media_type=self._get_voip_media_type(), parent=self)
        if dlg.exec_() == QDialog.Accepted:
            exp = dlg.get_expectation()
            self._expectations.append(exp)
            self._refresh_exp_table()
            self._emit_changed()

    def _edit_expectation(self):
        row = self.exp_table.currentRow()
        if row < 0 or row >= len(self._expectations):
            return
        protocol = self.protocol_combo.currentData()
        dlg = ExpectationDialog(protocol, self._expectations[row],
                                media_type=self._get_voip_media_type(), parent=self)
        if dlg.exec_() == QDialog.Accepted:
            self._expectations[row] = dlg.get_expectation()
            self._refresh_exp_table()
            self._emit_changed()

    def _remove_expectation(self):
        row = self.exp_table.currentRow()
        if row < 0 or row >= len(self._expectations):
            return
        self._expectations.pop(row)
        self._refresh_exp_table()
        self._emit_changed()

    def _refresh_exp_table(self):
        self.exp_table.setRowCount(len(self._expectations))
        for i, exp in enumerate(self._expectations):
            self.exp_table.setItem(i, 0, QTableWidgetItem(to_human(exp.get("metric", ""))))
            op = exp.get("operator", "")
            self.exp_table.setItem(i, 1, QTableWidgetItem(OPERATOR_LABELS.get(op, op)))
            val = exp.get("value", 0)
            self.exp_table.setItem(i, 2, QTableWidgetItem(str(int(val) if val == int(val) else val)))
            self.exp_table.setItem(i, 3, QTableWidgetItem(exp.get("unit", "")))
            self.exp_table.setItem(i, 4, QTableWidgetItem(to_human(exp.get("aggregation", ""))))
            self.exp_table.setItem(i, 5, QTableWidgetItem(to_human(exp.get("evaluation_scope", ""))))

    def get_scenario(self):
        """Build scenario dict from current form values."""
        scenario = {
            "id": self.id_edit.text().strip(),
            "description": self.desc_edit.text().strip(),
            "protocol": self.protocol_combo.currentData(),
            "schedule": {
                "mode": self.mode_combo.currentData(),
                "start_time": (self.start_time_input.text().strip()
                               if self.start_time_combo.currentData() == "iso"
                               else "immediate"),
            },
            "parameters": {},
            "expectations": list(self._expectations),
        }

        if self.mode_combo.currentData() == "recurring":
            scenario["schedule"]["interval_minutes"] = self.interval_spin.value()
            scenario["schedule"]["duration_hours"] = self.duration_spin.value()

        for key, (info, widget) in self.param_widgets.items():
            ptype = info["type"]
            if ptype == "list":
                scenario["parameters"][key] = widget.get_values()
            elif ptype == "str":
                val = widget.text().strip()
                scenario["parameters"][key] = [val] if info.get("wrap_list") and val else val
            elif ptype == "int":
                scenario["parameters"][key] = widget.value()
            elif ptype == "bool":
                scenario["parameters"][key] = widget.isChecked()
            elif ptype == "choice":
                scenario["parameters"][key] = widget.currentText()

        return scenario

    def load_scenario(self, s):
        """Populate form from scenario dict."""
        self._building = True
        self.id_edit.setText(s.get("id", ""))
        self.desc_edit.setText(s.get("description", ""))

        protocol = s.get("protocol", "speed_test")
        idx = self.protocol_combo.findData(protocol)
        if idx >= 0:
            self.protocol_combo.setCurrentIndex(idx)

        schedule = s.get("schedule", {})
        mode = schedule.get("mode", "once")
        idx = self.mode_combo.findData(mode)
        if idx >= 0:
            self.mode_combo.setCurrentIndex(idx)
        start_time = schedule.get("start_time", "immediate")
        if start_time and start_time.lower() != "immediate":
            self.start_time_combo.setCurrentIndex(
                self.start_time_combo.findData("iso"))
            self.start_time_input.setText(start_time)
        else:
            self.start_time_combo.setCurrentIndex(
                self.start_time_combo.findData("immediate"))
        if mode == "recurring":
            self.interval_spin.setValue(schedule.get("interval_minutes", 1))
            self.duration_spin.setValue(schedule.get("duration_hours", 1))

        # Load parameters
        params = s.get("parameters", {})
        for key, (info, widget) in self.param_widgets.items():
            if key not in params:
                continue
            ptype = info["type"]
            val = params[key]
            if ptype == "list" and isinstance(val, list):
                widget.set_values([str(v) for v in val])
            elif ptype == "str":
                if info.get("wrap_list") and isinstance(val, list):
                    widget.setText(str(val[0]) if val else "")
                else:
                    widget.setText(str(val))
            elif ptype == "int":
                widget.setValue(int(val))
            elif ptype == "bool":
                widget.setChecked(bool(val))
            elif ptype == "choice":
                ci = widget.findText(str(val))
                if ci >= 0:
                    widget.setCurrentIndex(ci)

        # Load expectations
        self._expectations = list(s.get("expectations", []))
        self._refresh_exp_table()
        self._building = False


# ---------------------------------------------------------------------------
# Configuration Tab
# ---------------------------------------------------------------------------
class ConfigurationTab(QWidget):
    """Main configuration editing tab."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)

        # -- Configuration name --
        name_row = QHBoxLayout()
        name_label = QLabel("Configuration Name:")
        name_label.setProperty("class", "subheading")
        name_row.addWidget(name_label)
        self.config_name_edit = QLineEdit()
        self.config_name_edit.setPlaceholderText("e.g., Production Baseline Test")
        self.config_name_edit.setMaxLength(255)
        self.config_name_edit.setMinimumWidth(350)
        name_row.addWidget(self.config_name_edit)
        name_row.addStretch()
        layout.addLayout(name_row)

        # -- Top bar: actions --
        top_bar = QHBoxLayout()
        top_bar.addStretch()

        # Action buttons
        btn_col = QVBoxLayout()
        load_btn = QPushButton(qta.icon("fa5s.folder-open", color=THEME_COLORS['text_primary']), " Load Config")
        load_btn.setProperty("class", "secondary")
        load_btn.clicked.connect(self._load_config)
        btn_col.addWidget(load_btn)

        save_btn = QPushButton(qta.icon("fa5s.save", color=THEME_COLORS['text_primary']), " Save Config")
        save_btn.clicked.connect(self._save_config)
        btn_col.addWidget(save_btn)
        top_bar.addLayout(btn_col)

        layout.addLayout(top_bar)

        # -- Scenario list + editor --
        content_row = QHBoxLayout()

        # Left: scenario list (fixed width)
        left_panel = QWidget()
        left_panel.setFixedWidth(420)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        lbl = QLabel("Scenarios")
        lbl.setProperty("class", "subheading")
        left_layout.addWidget(lbl)

        self.scenario_list = QListWidget()
        self.scenario_list.setIconSize(QSize(10, 10))
        self.scenario_list.currentRowChanged.connect(self._on_scenario_selected)
        self.scenario_list.itemChanged.connect(self._on_item_check_changed)
        left_layout.addWidget(self.scenario_list)

        list_btn_row = QHBoxLayout()
        add_btn = QPushButton(qta.icon("fa5s.plus", color=THEME_COLORS['text_primary']), " Add")
        add_btn.clicked.connect(self._add_scenario)
        list_btn_row.addWidget(add_btn)

        dup_btn = QPushButton(qta.icon("fa5s.copy", color=THEME_COLORS['text_primary']), " Duplicate")
        dup_btn.setProperty("class", "secondary")
        dup_btn.clicked.connect(self._duplicate_scenario)
        list_btn_row.addWidget(dup_btn)

        del_btn = QPushButton(qta.icon("fa5s.trash-alt", color=THEME_COLORS['error']), " Delete")
        del_btn.setProperty("class", "danger")
        del_btn.clicked.connect(self._delete_scenario)
        list_btn_row.addWidget(del_btn)

        left_layout.addLayout(list_btn_row)
        content_row.addWidget(left_panel)

        # Right: scenario editor stack
        self.editor_stack = QStackedWidget()
        self.empty_label = QLabel("Select or add a scenario to begin editing.")
        self.empty_label.setAlignment(Qt.AlignCenter)
        self.empty_label.setStyleSheet(f"color: {THEME_COLORS['text_muted']}; font-size: 16px;")
        self.editor_stack.addWidget(self.empty_label)
        content_row.addWidget(self.editor_stack, 1)

        layout.addLayout(content_row)

        # Internal state
        self._editors = []  # list of ScenarioEditor
        self._current_index = -1

    def _add_scenario(self):
        editor = ScenarioEditor()
        editor.id_edit.setText(f"new_scenario_{len(self._editors) + 1}")
        editor.changed.connect(self._on_editor_changed)
        self._editors.append(editor)
        self.editor_stack.addWidget(editor)

        item = QListWidgetItem(self._status_icon(True), editor.id_edit.text())
        item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
        item.setCheckState(Qt.Checked)
        self.scenario_list.addItem(item)
        self.scenario_list.setCurrentRow(self.scenario_list.count() - 1)

    def _duplicate_scenario(self):
        idx = self.scenario_list.currentRow()
        if idx < 0 or idx >= len(self._editors):
            return
        data = self._editors[idx].get_scenario()
        data["id"] = data["id"] + "_copy"
        # Carry over enabled state from the source item's checkbox
        src_item = self.scenario_list.item(idx)
        enabled = src_item.checkState() == Qt.Checked if src_item else True
        editor = ScenarioEditor(data)
        editor.changed.connect(self._on_editor_changed)
        self._editors.append(editor)
        self.editor_stack.addWidget(editor)
        item = QListWidgetItem(self._status_icon(enabled), data["id"])
        item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
        item.setCheckState(Qt.Checked if enabled else Qt.Unchecked)
        self.scenario_list.addItem(item)
        self.scenario_list.setCurrentRow(self.scenario_list.count() - 1)

    def _delete_scenario(self):
        idx = self.scenario_list.currentRow()
        if idx < 0 or idx >= len(self._editors):
            return
        reply = QMessageBox.question(
            self, "Delete Scenario",
            f"Delete scenario '{self._editors[idx].id_edit.text()}'?",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            editor = self._editors.pop(idx)
            self.editor_stack.removeWidget(editor)
            editor.deleteLater()
            self.scenario_list.takeItem(idx)
            if self.scenario_list.count() > 0:
                self.scenario_list.setCurrentRow(min(idx, self.scenario_list.count() - 1))
            else:
                self.editor_stack.setCurrentWidget(self.empty_label)

    def _on_scenario_selected(self, row):
        self._current_index = row
        if 0 <= row < len(self._editors):
            self.editor_stack.setCurrentWidget(self._editors[row])
        else:
            self.editor_stack.setCurrentWidget(self.empty_label)

    @staticmethod
    def _status_icon(enabled):
        color = THEME_COLORS['success'] if enabled else THEME_COLORS['error']
        return qta.icon("fa5s.circle", color=color)

    def _on_editor_changed(self):
        """Update scenario list label when editor changes."""
        idx = self.scenario_list.currentRow()
        if 0 <= idx < len(self._editors):
            editor = self._editors[idx]
            sid = editor.id_edit.text().strip() or "(unnamed)"
            item = self.scenario_list.item(idx)
            enabled = item.checkState() == Qt.Checked
            item.setText(sid)
            item.setIcon(self._status_icon(enabled))

    def _on_item_check_changed(self, item):
        """Update status icon when enabled checkbox is toggled in the list."""
        enabled = item.checkState() == Qt.Checked
        item.setIcon(self._status_icon(enabled))

    def get_config(self):
        """Build full configuration dict."""
        scenarios = []
        for i, editor in enumerate(self._editors):
            s = editor.get_scenario()
            item = self.scenario_list.item(i)
            s["enabled"] = item.checkState() == Qt.Checked if item else True
            scenarios.append(s)
        global_settings = {}
        config_name = self.config_name_edit.text().strip()
        if config_name:
            global_settings["name"] = config_name
        config = {
            "global_settings": global_settings,
            "scenarios": scenarios,
        }
        return config

    def load_config_data(self, config):
        """Load a configuration dict into the editor."""
        # Restore configuration name
        config_name = config.get("global_settings", {}).get("name", "")
        self.config_name_edit.setText(config_name)

        # Clear existing
        for editor in self._editors:
            self.editor_stack.removeWidget(editor)
            editor.deleteLater()
        self._editors.clear()
        self.scenario_list.clear()

        # Scenarios
        for s in config.get("scenarios", []):
            editor = ScenarioEditor(s)
            editor.changed.connect(self._on_editor_changed)
            self._editors.append(editor)
            self.editor_stack.addWidget(editor)
            enabled = s.get("enabled", True)
            item = QListWidgetItem(self._status_icon(enabled), s.get("id", "(unnamed)"))
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if enabled else Qt.Unchecked)
            self.scenario_list.addItem(item)

        if self.scenario_list.count() > 0:
            self.scenario_list.setCurrentRow(0)

    def _load_config(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Configuration", "configurations/", "JSON Files (*.json)"
        )
        if path:
            try:
                with open(path, "r") as f:
                    config = json.load(f)
            except json.JSONDecodeError as e:
                QMessageBox.critical(self, "Error", f"Invalid JSON:\n{e}")
                return
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to load config:\n{e}")
                return

            # Validate the loaded configuration
            from src.utils.config_validator import ConfigValidator
            validator = ConfigValidator()
            errors = validator.validate(config, skip_time_checks=True)
            if errors:
                error_msg = f"Configuration has {len(errors)} validation error(s):\n\n"
                error_msg += "\n".join(f"  - {e}" for e in errors[:20])
                if len(errors) > 20:
                    error_msg += f"\n  ... and {len(errors) - 20} more"
                QMessageBox.warning(self, "Validation Errors", error_msg)
                return

            self.load_config_data(config)

    def _save_config(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Configuration", "configurations/main.json", "JSON Files (*.json)"
        )
        if path:
            try:
                config = self.get_config()
                with open(path, "w") as f:
                    json.dump(config, f, indent=2)
                QMessageBox.information(self, "Saved", f"Configuration saved to:\n{path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save config:\n{e}")


# ---------------------------------------------------------------------------
# Test Runner Tab
# ---------------------------------------------------------------------------
class TestRunnerTab(QWidget):
    """Tab for running orchestrate.py and viewing live output."""

    _MAX_CONSOLE_LINES = 5000
    _TRIM_TO = 3000

    def __init__(self, config_tab: ConfigurationTab, parent=None):
        super().__init__(parent)
        self.config_tab = config_tab
        self.process = None
        self._last_console_line = ""
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)

        # -- Controls --
        ctrl_row = QHBoxLayout()

        self.run_btn = QPushButton(qta.icon("fa5s.rocket", color="#ffffff"), "  Run Tests  ")
        self.run_btn.setStyleSheet(
            f"QPushButton {{ font-size: 16px; padding: 14px 44px; border-radius: 24px; "
            f"background: qlineargradient(x1:0, y1:0, x2:1, y2:1, "
            f"stop:0 {THEME_COLORS['red_primary']}, stop:1 {THEME_COLORS['red_dark']}); "
            f"letter-spacing: 1.5px; font-weight: 800; border: 1px solid {THEME_COLORS['red_primary']}; }}"
            f"QPushButton:hover {{ background: qlineargradient(x1:0, y1:0, x2:1, y2:1, "
            f"stop:0 {THEME_COLORS['red_hover']}, stop:1 {THEME_COLORS['red_primary']}); "
            f"border: 1px solid {THEME_COLORS['red_hover']}; }}"
            f"QPushButton:disabled {{ background: {THEME_COLORS['bg_light']}; "
            f"color: {THEME_COLORS['text_muted']}; border: 1px solid {THEME_COLORS['border']}; }}"
        )
        self.run_btn.clicked.connect(self._run_tests)
        ctrl_row.addWidget(self.run_btn)

        self.stop_btn = QPushButton(qta.icon("fa5s.stop-circle", color=THEME_COLORS['error']), " Stop")
        self.stop_btn.setProperty("class", "danger")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._stop_tests)
        ctrl_row.addWidget(self.stop_btn)

        ctrl_row.addStretch()

        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet(f"color: {THEME_COLORS['text_secondary']}; font-size: 15px;")
        ctrl_row.addWidget(self.status_label)

        layout.addLayout(ctrl_row)

        # -- Spinner --
        self._spin_widget = qta.IconWidget()
        self._spin_widget.setFixedSize(40, 40)
        self._spin_widget.setStyleSheet("background: transparent;")
        self._spin_widget.setVisible(False)
        self._spin_animation = qta.Spin(self._spin_widget, interval=50, step=5)
        self._spin_widget.setIcon(qta.icon("fa5s.circle-notch", color=THEME_COLORS['success'], animation=self._spin_animation))
        self._spin_widget.setIconSize(QSize(34, 34))
        ctrl_row.addWidget(self._spin_widget)

        # -- Output area: console (left) + error panel (right) --
        output_splitter = QSplitter(Qt.Horizontal)

        # Console (left side)
        console_widget = QWidget()
        console_vlayout = QVBoxLayout(console_widget)
        console_vlayout.setContentsMargins(0, 0, 0, 0)
        console_vlayout.setSpacing(4)

        console_hbox = QHBoxLayout()
        console_icon = QLabel()
        console_icon.setPixmap(qta.icon("fa5s.terminal", color=THEME_COLORS['text_secondary']).pixmap(16, 16))
        console_icon.setStyleSheet("background: transparent;")
        console_hbox.addWidget(console_icon)
        console_label = QLabel("Console Output")
        console_label.setProperty("class", "subheading")
        console_hbox.addWidget(console_label)
        console_hbox.addStretch()
        console_vlayout.addLayout(console_hbox)

        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setMinimumHeight(300)
        console_vlayout.addWidget(self.console, 1)

        output_splitter.addWidget(console_widget)

        # Error panel (right side) — hidden by default
        self._error_panel = QWidget()
        error_vlayout = QVBoxLayout(self._error_panel)
        error_vlayout.setContentsMargins(0, 0, 0, 0)
        error_vlayout.setSpacing(4)

        error_hbox = QHBoxLayout()
        error_icon = QLabel()
        error_icon.setPixmap(qta.icon("fa5s.exclamation-circle", color=THEME_COLORS['error']).pixmap(16, 16))
        error_icon.setStyleSheet("background: transparent;")
        error_hbox.addWidget(error_icon)
        error_heading = QLabel("Configuration Errors")
        error_heading.setStyleSheet(
            f"color: {THEME_COLORS['error']}; font-size: 15px; font-weight: 800;"
        )
        error_hbox.addWidget(error_heading)
        error_hbox.addStretch()
        error_vlayout.addLayout(error_hbox)

        self._error_display = QTextEdit()
        self._error_display.setReadOnly(True)
        self._error_display.setStyleSheet(
            f"background-color: {THEME_COLORS['bg_card']}; "
            f"color: {THEME_COLORS['error']}; "
            f"font-family: 'Segoe UI', 'Arial', sans-serif; "
            f"font-size: 14px; "
            f"padding: 14px; "
            f"border: 2px solid {THEME_COLORS['error']}; "
            f"border-radius: 12px;"
        )
        error_vlayout.addWidget(self._error_display, 1)

        output_splitter.addWidget(self._error_panel)
        self._error_panel.setVisible(False)

        output_splitter.setStretchFactor(0, 3)
        output_splitter.setStretchFactor(1, 2)

        layout.addWidget(output_splitter, 1)

    # Lines produced by ``docker service create`` progress reporting that
    # should be hidden from the user – they only care about orchestrate.py's
    # own print statements.
    _DOCKER_NOISE_PATTERNS = (
        "overall progress:",
        "verify: ",
        "converged",
        "image ",
        "its digest",
        "possibly leading to",
        "versions of the image",
    )

    def _run_tests(self):
        # Save config before running
        config = self.config_tab.get_config()
        config_path = os.path.join(os.path.dirname(__file__), "configurations", "main.json")
        try:
            with open(config_path, "w") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save config:\n{e}")
            return

        # Validate configuration before launching
        try:
            from src.utils.config_validator import validate_config_file
            is_valid, errors = validate_config_file(config_path)
        except Exception as e:
            is_valid, errors = False, [f"Validator failed to load: {e}"]

        if not is_valid:
            self._show_config_errors(errors)
            return

        # Hide error panel from any previous failed run
        self._error_panel.setVisible(False)

        self.console.clear()
        self._last_console_line = ""
        self.console.append(f"[INFO] Configuration saved to {config_path}")
        self.console.append("[INFO] Starting orchestrate.py...\n")

        self.run_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self._spin_widget.setVisible(True)
        self._spin_animation.start()
        self.status_label.setText("Running...")
        self.status_label.setStyleSheet(f"color: {THEME_COLORS['red_primary']}; font-size: 15px; font-weight: bold;")

        self.process = QProcess(self)
        self.process.setWorkingDirectory(os.path.dirname(os.path.abspath(__file__)))
        self.process.setProcessChannelMode(QProcess.MergedChannels)
        self.process.readyReadStandardOutput.connect(self._read_output)
        self.process.finished.connect(self._on_finished)
        # -u = unbuffered so we get live output line-by-line
        self.process.start("python3", ["-u", "orchestrate.py"])

    def _stop_tests(self):
        if self.process and self.process.state() != QProcess.NotRunning:
            self.process.kill()
            self.console.append("\n[INFO] Process killed by user.")
            self.console.append("[INFO] Cleaning up Docker services...")
            self._cleanup_docker_services()

    def _cleanup_docker_services(self):
        """Remove loadtest-* Docker Swarm services orphaned by killing the orchestrator."""
        def _do_cleanup():
            try:
                result = subprocess.run(
                    ["docker", "service", "ls", "--filter", "name=loadtest-", "--format", "{{.Name}}"],
                    capture_output=True, text=True, timeout=15,
                )
                services = [s.strip() for s in result.stdout.strip().split("\n") if s.strip()]
                for svc in services:
                    subprocess.run(
                        ["docker", "service", "rm", svc],
                        capture_output=True, text=True, timeout=30,
                    )
            except Exception:
                pass

        cleanup_thread = threading.Thread(target=_do_cleanup, daemon=True)
        cleanup_thread.start()

    def _is_docker_noise(self, line: str) -> bool:
        """Return True if the line is Docker service-create progress noise."""
        stripped = line.strip()
        if not stripped:
            return False
        # Docker service IDs are 25-char hex strings on their own line
        if len(stripped) == 25 and all(c in "0123456789abcdef" for c in stripped):
            return True
        # Bare task progress lines like "1/1:  " or "1/1: running"
        if stripped.startswith("1/") and ":" in stripped and len(stripped) < 40:
            return True
        for pattern in self._DOCKER_NOISE_PATTERNS:
            if pattern in stripped:
                return True
        return False

    def _read_output(self):
        data = self.process.readAllStandardOutput().data().decode("utf-8", errors="replace")
        # Filter out docker service create noise and duplicate consecutive lines
        filtered_lines = []
        for line in data.splitlines(keepends=True):
            if self._is_docker_noise(line):
                continue
            stripped = line.rstrip()
            if stripped and stripped == self._last_console_line:
                continue
            if stripped:
                self._last_console_line = stripped
            filtered_lines.append(line)
        if filtered_lines:
            text = "".join(filtered_lines)
            self.console.moveCursor(self.console.textCursor().End)
            self.console.insertPlainText(text)
            self.console.moveCursor(self.console.textCursor().End)

            # Trim console if it exceeds max lines to prevent memory bloat
            doc = self.console.document()
            if doc.blockCount() > self._MAX_CONSOLE_LINES:
                cursor = self.console.textCursor()
                cursor.movePosition(cursor.Start)
                cursor.movePosition(cursor.Down, cursor.KeepAnchor,
                                    doc.blockCount() - self._TRIM_TO)
                cursor.removeSelectedText()

    def _on_finished(self, exit_code, exit_status):
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._spin_animation.stop()
        self._spin_widget.setVisible(False)

        if exit_code == 0:
            self.status_label.setText("Completed Successfully")
            self.status_label.setStyleSheet(f"color: {THEME_COLORS['success']}; font-size: 15px; font-weight: bold;")
            self.console.append("\n[INFO] Orchestration completed successfully.")
        else:
            self.status_label.setText(f"Failed (exit code {exit_code})")
            self.status_label.setStyleSheet(f"color: {THEME_COLORS['error']}; font-size: 15px; font-weight: bold;")
            self.console.append(f"\n[ERROR] Process exited with code {exit_code}")

    def _show_config_errors(self, errors: list):
        """Display configuration validation errors in the error panel."""
        self.console.clear()
        self.console.append("[ERROR] Configuration validation failed. See error panel for details.\n")
        self.status_label.setText("Configuration Invalid")
        self.status_label.setStyleSheet(f"color: {THEME_COLORS['error']}; font-size: 15px; font-weight: bold;")

        # Build user-friendly error text
        lines = []
        lines.append(f"Found {len(errors)} configuration error(s).\n")
        lines.append("Please fix the following issues in the Configuration tab before running tests:\n")
        for i, error in enumerate(errors, 1):
            lines.append(f"  {i}.  {error}\n")
        lines.append("\nTests will not start until all errors are resolved.")

        self._error_display.setPlainText("\n".join(lines))
        self._error_panel.setVisible(True)


# ---------------------------------------------------------------------------
# Background workers for ResultsTab (keep the GUI thread free)
# ---------------------------------------------------------------------------

class _ResultsLoadWorker(QObject):
    """Run DB health-checks and data loading off the main thread."""

    finished = pyqtSignal()
    error = pyqtSignal(str, str)       # (dialog_title, message)
    info = pyqtSignal(str, str)        # (dialog_title, message)
    data_ready = pyqtSignal(object)    # dict with all loaded data

    _DB_CONTAINER = "db-container"
    _DB_VOLUME = "load-test"
    _DB_IMAGE = "postgres:16-alpine"

    def run(self):
        try:
            if not self._ensure_db_healthy():
                self.finished.emit()
                return
            self._fetch_data()
        except Exception as e:
            self.error.emit("Error", str(e))
        self.finished.emit()

    # -- Docker / Postgres health -------------------------------------------

    def _ensure_db_healthy(self) -> bool:
        # 1. Docker daemon reachable?
        try:
            res = subprocess.run(
                ["docker", "info"], capture_output=True, text=True, timeout=10,
            )
            if res.returncode != 0:
                self.error.emit(
                    "Docker Unavailable",
                    "The Docker daemon is not running or not accessible.\n\n"
                    "Please start Docker and try again.",
                )
                return False
        except FileNotFoundError:
            self.error.emit(
                "Docker Not Found",
                "The 'docker' command was not found on this system.\n\n"
                "Please install Docker and try again.",
            )
            return False
        except Exception as e:
            self.error.emit("Docker Error", f"Failed to query Docker daemon:\n{e}")
            return False

        # 2. db-container running?
        try:
            res = subprocess.run(
                ["docker", "ps", "-q", "-f", f"name={self._DB_CONTAINER}"],
                capture_output=True, text=True, timeout=10,
            )
            if not res.stdout.strip():
                if not self._start_db_container():
                    return False
        except Exception as e:
            self.error.emit("Docker Error", f"Failed to check container status:\n{e}")
            return False

        # 3. Postgres accepting connections?
        if not self._wait_for_postgres():
            self.error.emit(
                "Database Not Ready",
                "The PostgreSQL container is running but the database did not "
                "become ready in time.\n\n"
                "Please check the container logs and try again.",
            )
            return False

        return True

    def _start_db_container(self) -> bool:
        try:
            subprocess.run(
                ["docker", "volume", "create", self._DB_VOLUME],
                capture_output=True, text=True, timeout=30,
            )
            subprocess.run(
                ["docker", "rm", "-f", self._DB_CONTAINER],
                capture_output=True, text=True, timeout=30,
            )

            init_sql = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "docker", "init_db.sql",
            )

            run_cmd = [
                "docker", "run", "-d",
                "--name", self._DB_CONTAINER,
                "-e", "POSTGRES_PASSWORD=postgres",
                "-e", "POSTGRES_DB=postgres",
                "-v", f"{self._DB_VOLUME}:/var/lib/postgresql/data",
                "-p", "5432:5432",
            ]
            if os.path.isfile(init_sql):
                run_cmd += ["-v", f"{init_sql}:/docker-entrypoint-initdb.d/init_db.sql"]

            net_check = subprocess.run(
                ["docker", "network", "inspect", "loadtest-network"],
                capture_output=True, text=True, timeout=10,
            )
            if net_check.returncode == 0:
                run_cmd += ["--network", "loadtest-network"]

            run_cmd.append(self._DB_IMAGE)

            res = subprocess.run(run_cmd, capture_output=True, text=True, timeout=60)
            if res.returncode != 0:
                self.error.emit(
                    "Container Start Failed",
                    f"Failed to start the PostgreSQL container:\n{res.stderr.strip()}",
                )
                return False
            return True

        except Exception as e:
            self.error.emit(
                "Container Start Failed",
                f"An error occurred while starting the database container:\n{e}",
            )
            return False

    def _wait_for_postgres(self, max_retries: int = 15, delay: int = 2) -> bool:
        import time
        for _ in range(max_retries):
            try:
                res = subprocess.run(
                    ["docker", "exec", self._DB_CONTAINER,
                     "pg_isready", "-U", "postgres"],
                    capture_output=True, text=True, timeout=10,
                )
                if res.returncode == 0:
                    return True
            except Exception:
                pass
            time.sleep(delay)
        return False

    # -- Data fetching ------------------------------------------------------

    def _fetch_data(self):
        from src.utils.db import (
            get_latest_scenario_ids, get_scenarios, get_result_logs,
            get_test_runs, get_raw_metrics_for_run, get_scenario_summaries,
            get_distinct_config_names,
        )

        all_sids = get_latest_scenario_ids()
        if not all_sids:
            self.info.emit("No Data", "No scenarios found in the database.")
            return

        scenarios_raw = get_scenarios(all_sids)

        scenario_map = {}
        scenario_id_order = []
        for s in scenarios_raw:
            sid = str(s["scenario_id"])
            cfg = s.get("config_snapshot", {}) or {}
            scenario_map[sid] = {
                "name": cfg.get("id", sid[:8]),
                "protocol": s.get("protocol", cfg.get("protocol", "")),
                "description": cfg.get("description", ""),
                "config_name": s.get("config_name") or None,
            }
            scenario_id_order.append(sid)

        try:
            config_names = get_distinct_config_names()
        except Exception:
            config_names = []

        all_report_rows = []
        all_result_log_rows = []
        all_test_run_rows = []
        all_raw_metric_rows = []
        all_summary_rows = []

        for sid in all_sids:
            try:
                result_logs = get_result_logs(sid)
                test_runs = get_test_runs(sid)
                summaries = get_scenario_summaries(sid)
            except Exception:
                continue

            info = scenario_map.get(str(sid), {})

            for rl in result_logs:
                all_result_log_rows.append(rl)
                all_report_rows.append({
                    "scenario_id": str(sid),
                    "timestamp": rl.get("start_time"),
                    "cells": [
                        info.get("name", str(sid)[:8]),
                        info.get("protocol", ""),
                        rl.get("metric_name", ""),
                        str(rl.get("expected_value", "")),
                        str(rl.get("measured_value", "")),
                        rl.get("status", ""),
                        rl.get("scope", ""),
                        str(rl.get("start_time", "")),
                        info.get("description", ""),
                    ],
                })

            for tr in test_runs:
                all_test_run_rows.append(tr)
                try:
                    metrics = get_raw_metrics_for_run(str(tr["run_id"]))
                    all_raw_metric_rows.extend(metrics)
                except Exception:
                    pass

            all_summary_rows.extend(summaries)

        self.data_ready.emit({
            "scenarios_raw": scenarios_raw,
            "scenario_map": scenario_map,
            "scenario_id_order": scenario_id_order,
            "config_names": config_names,
            "all_report_rows": all_report_rows,
            "all_result_log_rows": all_result_log_rows,
            "all_test_run_rows": all_test_run_rows,
            "all_raw_metric_rows": all_raw_metric_rows,
            "all_summary_rows": all_summary_rows,
        })


class _NukeWorker(QObject):
    """Run database purge commands off the main thread."""

    finished = pyqtSignal()
    done = pyqtSignal(list)  # list of error strings (empty == success)

    def run(self):
        commands = [
            ["docker", "stop", "db-container"],
            ["docker", "rm", "db-container"],
            ["docker", "volume", "rm", "load-test"],
        ]
        errors = []
        for cmd in commands:
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if res.returncode != 0:
                    errors.append(f"{' '.join(cmd)}: {res.stderr.strip()}")
            except Exception as e:
                errors.append(f"{' '.join(cmd)}: {e}")
        self.done.emit(errors)
        self.finished.emit()


# ---------------------------------------------------------------------------
# Results Tab
# ---------------------------------------------------------------------------
class ResultsTab(QWidget):
    """Tab for displaying test results loaded directly from the database."""

    ERRORS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "errors")

    def __init__(self, config_tab=None, parent=None):
        super().__init__(parent)
        self._config_tab = config_tab
        self._all_report_rows = []
        self._report_headers = [
            "Scenario", "Protocol", "Metric", "Expected", "Measured",
            "Status", "Scope", "Timestamp", "Description",
        ]
        self._scenario_map = {}
        self._scenario_id_order = []
        self._config_names = []  # list of {"config_name": ..., "scenario_id": ...}
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        # ── Controls row ──
        ctrl = QHBoxLayout()

        self._load_btn = QPushButton(qta.icon("fa5s.sync-alt", color=THEME_COLORS['text_primary']), " Load Results")
        self._load_btn.clicked.connect(self._load_results)
        ctrl.addWidget(self._load_btn)

        export_btn = QPushButton(qta.icon("fa5s.file-export", color=THEME_COLORS['text_primary']), " Export to CSV")
        export_btn.clicked.connect(self._export_to_csv)
        ctrl.addWidget(export_btn)

        import_btn = QPushButton(qta.icon("fa5s.file-import", color=THEME_COLORS['text_primary']), " Import CSV")
        import_btn.clicked.connect(self._load_from_csv)
        ctrl.addWidget(import_btn)

        ctrl.addStretch()

        self._nuke_btn = QPushButton(qta.icon("fa5s.bomb", color=THEME_COLORS['error']), " Purge Database")
        self._nuke_btn.setProperty("class", "danger")
        self._nuke_btn.clicked.connect(self._nuke_database)
        ctrl.addWidget(self._nuke_btn)

        layout.addLayout(ctrl)

        # ── Sub-tabs ──
        self.result_tabs = QTabWidget()
        self.result_tabs.tabBar().setExpanding(False)
        self.result_tabs.setUsesScrollButtons(True)
        layout.addWidget(self.result_tabs)

        # ── Summary panel ──
        self.summary_group = QGroupBox("Test Summary")
        summary_layout = QVBoxLayout()
        self.summary_label = QLabel("Load results to see summary.")
        self.summary_label.setWordWrap(True)
        self.summary_label.setStyleSheet(f"font-size: 15px; color: {THEME_COLORS['text_secondary']};")
        summary_layout.addWidget(self.summary_label)
        self.summary_group.setLayout(summary_layout)
        layout.addWidget(self.summary_group)

    # ── Load from DB (threaded) ──

    def _load_results(self):
        """Kick off a background thread that checks Docker/DB health and
        fetches all result data.  The GUI stays fully responsive."""
        self._load_btn.setEnabled(False)
        self.summary_label.setText("Loading results...")
        self.summary_label.setStyleSheet(
            f"font-size: 15px; color: {THEME_COLORS['text_secondary']};"
        )

        self._load_thread = QThread()
        self._load_worker = _ResultsLoadWorker()
        self._load_worker.moveToThread(self._load_thread)

        self._load_thread.started.connect(self._load_worker.run)
        self._load_worker.data_ready.connect(self._on_results_data_ready)
        self._load_worker.error.connect(self._on_results_error)
        self._load_worker.info.connect(self._on_results_info)
        self._load_worker.finished.connect(self._load_thread.quit)
        self._load_worker.finished.connect(self._on_load_finished)

        self._load_thread.start()

    def _on_results_data_ready(self, payload):
        self._scenario_map = payload["scenario_map"]
        self._scenario_id_order = payload["scenario_id_order"]
        self._config_names = payload["config_names"]
        self._all_report_rows = payload["all_report_rows"]
        self._build_result_tabs(
            payload["scenarios_raw"],
            payload["all_result_log_rows"],
            payload["all_summary_rows"],
            payload["all_test_run_rows"],
            payload["all_raw_metric_rows"],
        )

    def _on_results_error(self, title, msg):
        QMessageBox.critical(self, title, msg)

    def _on_results_info(self, title, msg):
        QMessageBox.information(self, title, msg)

    def _on_load_finished(self):
        self._load_btn.setEnabled(True)
        if self.summary_label.text() == "Loading results...":
            self.summary_label.setText("Load results to see summary.")

    def _build_result_tabs(self, scenarios_raw, all_result_log_rows, all_summary_rows,
                            all_test_run_rows, all_raw_metric_rows):
        """Build all result sub-tabs from the collected data."""
        self.result_tabs.clear()
        _ic = THEME_COLORS['text_secondary']

        # Tab 0: Expectation Report (with filters — default shows latest scenario)
        self._build_expectation_report_tab()

        # Tab: Scenarios
        if scenarios_raw:
            sc_headers = ["scenario_id", "protocol", "config_snapshot"]
            self._add_db_table_tab(
                "Scenarios", qta.icon("fa5s.layer-group", color=_ic),
                sc_headers, scenarios_raw
            )

        # Tab: Results Log
        if all_result_log_rows:
            rl_headers = ["run_id", "metric_name", "expected_value", "measured_value", "status", "scope"]
            self._add_db_table_tab(
                "Results Log", qta.icon("fa5s.clipboard-list", color=_ic),
                rl_headers, all_result_log_rows, status_col_name="status"
            )

        # Tab: Scenario Summary
        if all_summary_rows:
            sum_headers = ["scenario_id", "metric_name", "sample_count", "avg_value",
                           "min_value", "max_value", "percentile", "percentile_result",
                           "stddev_value", "aggregated_at"]
            self._add_db_table_tab(
                "Scenario Summary", qta.icon("fa5s.list-alt", color=_ic),
                sum_headers, all_summary_rows
            )

        # Tab: Test Runs
        if all_test_run_rows:
            tr_headers = ["run_id", "scenario_id", "start_time", "worker_node"]
            self._add_db_table_tab(
                "Test Runs", qta.icon("fa5s.running", color=_ic),
                tr_headers, all_test_run_rows
            )

        # Tab: Raw Metrics
        if all_raw_metric_rows:
            rm_headers = ["metric_name", "metric_value", "timestamp"]
            self._add_db_table_tab(
                "Raw Metrics", qta.icon("fa5s.database", color=_ic),
                rm_headers, all_raw_metric_rows
            )

        # Tab: Error Log
        self._build_error_log_tab()

        # Select expectation report tab
        self.result_tabs.setCurrentIndex(0)

    # ── Load from CSV ──
    def _load_from_csv(self):
        """Load results from previously exported CSV directory and display them."""
        import csv

        csv_dir = QFileDialog.getExistingDirectory(self, "Select CSV Export Directory")
        if not csv_dir:
            return

        def read_csv_file(filepath):
            """Read a CSV file and return (headers, list-of-dicts)."""
            if not os.path.isfile(filepath):
                return [], []
            with open(filepath, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            headers = reader.fieldnames or []
            return headers, rows

        # Read all exported CSV files
        _, scenarios_rows = read_csv_file(os.path.join(csv_dir, "scenarios.csv"))
        _, test_run_rows = read_csv_file(os.path.join(csv_dir, "test_runs.csv"))
        _, raw_metric_rows = read_csv_file(os.path.join(csv_dir, "raw_metrics.csv"))
        _, result_log_rows = read_csv_file(os.path.join(csv_dir, "results_log.csv"))
        _, summary_rows = read_csv_file(os.path.join(csv_dir, "scenario_summary.csv"))

        if not scenarios_rows and not result_log_rows:
            QMessageBox.information(self, "No Data",
                                    "No CSV data found in the selected directory.\n\n"
                                    "Expected files: scenarios.csv, test_runs.csv, "
                                    "raw_metrics.csv, results_log.csv, scenario_summary.csv")
            return

        # Build scenario map from scenarios.csv
        self._scenario_map = {}
        self._scenario_id_order = []
        for s in scenarios_rows:
            sid = s.get("scenario_id", "")
            config_str = s.get("config_snapshot", "{}")
            try:
                config = json.loads(config_str) if config_str else {}
            except (json.JSONDecodeError, TypeError):
                config = {}
            self._scenario_map[sid] = {
                "name": config.get("id", sid[:8]),
                "protocol": s.get("protocol", config.get("protocol", "")),
                "description": config.get("description", ""),
            }
            self._scenario_id_order.append(sid)

        # Build a run_id -> scenario_id lookup from test_runs
        run_to_scenario = {}
        for tr in test_run_rows:
            run_to_scenario[tr.get("run_id", "")] = tr.get("scenario_id", "")

        # Build expectation report rows from results_log + test_runs
        self._all_report_rows = []
        for rl in result_log_rows:
            run_id = rl.get("run_id", "")
            sid = run_to_scenario.get(run_id, "")
            info = self._scenario_map.get(sid, {})
            # Find matching test_run for timestamp
            ts = ""
            for tr in test_run_rows:
                if tr.get("run_id") == run_id:
                    ts = tr.get("start_time", "")
                    break
            self._all_report_rows.append({
                "scenario_id": sid,
                "timestamp": ts,
                "cells": [
                    info.get("name", sid[:8] if sid else ""),
                    info.get("protocol", ""),
                    rl.get("metric_name", ""),
                    rl.get("expected_value", ""),
                    rl.get("measured_value", ""),
                    rl.get("status", ""),
                    rl.get("scope", ""),
                    ts,
                    info.get("description", ""),
                ],
            })

        self._build_result_tabs(scenarios_rows, result_log_rows, summary_rows,
                                test_run_rows, raw_metric_rows)

    def _add_db_table_tab(self, tab_name, tab_icon, headers, rows, status_col_name=None):
        """Create a QTableWidget tab from DB rows (list of dicts)."""
        table = QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(rows))
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)

        status_col = headers.index(status_col_name) if status_col_name and status_col_name in headers else -1

        for r, row in enumerate(rows):
            for c, col_name in enumerate(headers):
                val = str(row.get(col_name, ""))
                item = QTableWidgetItem(val)
                if c == status_col:
                    upper = val.upper()
                    if upper == "PASS":
                        item.setForeground(QColor(THEME_COLORS["success"]))
                        item.setFont(QFont("", -1, QFont.Bold))
                    elif upper == "FAIL":
                        item.setForeground(QColor(THEME_COLORS["error"]))
                        item.setFont(QFont("", -1, QFont.Bold))
                    elif upper == "ERROR":
                        item.setForeground(QColor(THEME_COLORS["warning"]))
                        item.setFont(QFont("", -1, QFont.Bold))
                table.setItem(r, c, item)

        self.result_tabs.addTab(table, tab_icon, tab_name)

    # ── Expectation Report ──
    def _build_expectation_report_tab(self):
        """Build the Expectation Report tab with scenario/protocol/date filters.
        Defaults to showing only the latest scenario."""
        wrapper = QWidget()
        wrapper_layout = QVBoxLayout(wrapper)
        wrapper_layout.setContentsMargins(0, 0, 0, 0)
        wrapper_layout.setSpacing(8)

        _fs = f"color: {THEME_COLORS['text_secondary']}; font-weight: 700;"

        # Filter row 1: configuration + scenario + protocol
        filter_row = QHBoxLayout()

        config_label = QLabel("Configuration:")
        config_label.setStyleSheet(_fs)
        filter_row.addWidget(config_label)

        self._report_config_combo = QComboBox()
        self._report_config_combo.setMinimumWidth(300)
        self._report_config_combo.addItem("All Configurations", "")
        # Determine current config name from the Configuration tab
        current_config_name = None
        if self._config_tab:
            try:
                cfg = self._config_tab.get_config()
                current_config_name = cfg.get("global_settings", {}).get("name")
            except Exception:
                pass
        default_config_index = 0
        for cn_row in self._config_names:
            cn = cn_row.get("config_name")
            sid = str(cn_row.get("scenario_id", ""))
            display_name = cn if cn else "(unnamed)"
            label = f"{display_name} ({sid[:8]})" if sid else display_name
            self._report_config_combo.addItem(label, cn if cn else "__unnamed__")
            if cn and cn == current_config_name:
                default_config_index = self._report_config_combo.count() - 1
        if default_config_index > 0:
            self._report_config_combo.setCurrentIndex(default_config_index)
        self._report_config_combo.currentIndexChanged.connect(self._apply_report_filter)
        filter_row.addWidget(self._report_config_combo)

        filter_label = QLabel("Scenario:")
        filter_label.setStyleSheet(_fs)
        filter_row.addWidget(filter_label)

        self._report_filter_combo = QComboBox()
        self._report_filter_combo.setMinimumWidth(420)
        self._report_filter_combo.addItem("All Scenarios", "")
        self._latest_scenario_id = self._scenario_id_order[0] if self._scenario_id_order else ""
        if self._scenario_id_order:
            latest_sid = self._scenario_id_order[0]
            self._report_filter_combo.addItem(f"Latest  ({latest_sid})", "__latest__")
        for sid in self._scenario_id_order:
            info = self._scenario_map.get(sid, {})
            name = info.get("name", "")
            label = f"{sid}  ({name})" if name else sid
            self._report_filter_combo.addItem(label, sid)
        # Default to "All Scenarios" when a config is selected, else "Latest"
        if default_config_index > 0:
            self._report_filter_combo.setCurrentIndex(0)
        elif self._scenario_id_order:
            self._report_filter_combo.setCurrentIndex(1)
        self._report_filter_combo.currentIndexChanged.connect(self._apply_report_filter)
        filter_row.addWidget(self._report_filter_combo)

        proto_label = QLabel("Protocol:")
        proto_label.setStyleSheet(_fs)
        filter_row.addWidget(proto_label)

        self._report_proto_combo = QComboBox()
        self._report_proto_combo.setMinimumWidth(180)
        self._report_proto_combo.addItem("All Protocols", "")
        for proto in PROTOCOLS:
            self._report_proto_combo.addItem(to_human(proto), proto)
        self._report_proto_combo.currentIndexChanged.connect(self._apply_report_filter)
        filter_row.addWidget(self._report_proto_combo)

        filter_row.addStretch()
        wrapper_layout.addLayout(filter_row)

        # Filter row 2: date/time range + runs
        date_row = QHBoxLayout()

        from_label = QLabel("From:")
        from_label.setStyleSheet(_fs)
        date_row.addWidget(from_label)

        self.from_dt = QDateTimeEdit()
        self.from_dt.setCalendarPopup(True)
        self.from_dt.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.from_dt.setDateTime(QDateTime.currentDateTime().addDays(-7))
        self.from_dt.dateTimeChanged.connect(self._apply_report_filter)
        date_row.addWidget(self.from_dt)

        to_label = QLabel("To:")
        to_label.setStyleSheet(_fs)
        date_row.addWidget(to_label)

        self.to_dt = QDateTimeEdit()
        self.to_dt.setCalendarPopup(True)
        self.to_dt.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.to_dt.setDateTime(QDateTime.currentDateTime())
        self.to_dt.dateTimeChanged.connect(self._apply_report_filter)
        date_row.addWidget(self.to_dt)

        runs_label = QLabel("Runs:")
        runs_label.setStyleSheet(_fs)
        date_row.addWidget(runs_label)

        self._report_runs_combo = QComboBox()
        self._report_runs_combo.setMinimumWidth(160)
        self._report_runs_combo.addItem("Latest Run Only", "latest")
        self._report_runs_combo.addItem("All Past Runs", "all")
        self._report_runs_combo.currentIndexChanged.connect(self._apply_report_filter)
        date_row.addWidget(self._report_runs_combo)

        date_row.addStretch()
        wrapper_layout.addLayout(date_row)

        # Table
        self._report_table = QTableWidget()
        self._report_table.setColumnCount(len(self._report_headers))
        self._report_table.setHorizontalHeaderLabels(self._report_headers)
        self._report_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._report_table.setAlternatingRowColors(True)
        self._report_table.verticalHeader().setVisible(False)
        self._report_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        wrapper_layout.addWidget(self._report_table)

        # Apply default filter (latest scenario) and populate
        self._apply_report_filter()

        self.result_tabs.insertTab(
            0, wrapper,
            qta.icon("fa5s.check-double", color=THEME_COLORS['text_secondary']),
            "Expectation Report"
        )

    # ── Error Log from ./errors/ ──
    def _build_error_log_tab(self):
        """Load error files from ./errors/ directory and display them."""
        errors_dir = self.ERRORS_DIR
        if not os.path.isdir(errors_dir):
            return

        combined_content = ""
        try:
            for filename in sorted(os.listdir(errors_dir)):
                if not filename.endswith(".txt"):
                    continue
                filepath = os.path.join(errors_dir, filename)
                try:
                    with open(filepath, "r") as f:
                        content = f.read().strip()
                except Exception:
                    continue
                if content:
                    scenario_label = filename.replace(".txt", "")
                    combined_content += f"{'=' * 60}\n  {scenario_label}\n{'=' * 60}\n{content}\n\n"
        except Exception:
            return

        if not combined_content.strip():
            return

        log_display = QTextEdit()
        log_display.setReadOnly(True)
        log_display.setPlainText(combined_content)
        log_display.setStyleSheet(
            f"background-color: {THEME_COLORS['bg_card']}; "
            f"color: {THEME_COLORS['error']}; "
            f"font-family: 'Cascadia Code', 'Fira Code', 'JetBrains Mono', 'Consolas', monospace; "
            f"font-size: 13px; "
            f"padding: 14px; "
            f"border: 1px solid {THEME_COLORS['border']}; "
            f"border-radius: 12px;"
        )

        self.result_tabs.addTab(
            log_display,
            qta.icon("fa5s.exclamation-triangle", color=THEME_COLORS['warning']),
            "Error Log"
        )

    def _populate_report_table(self, rows):
        """Fill the expectation report table with the given row dicts."""
        table = self._report_table
        table.setRowCount(len(rows))
        status_col = self._report_headers.index("Status")
        for r, entry in enumerate(rows):
            cells = entry["cells"]
            for c, val in enumerate(cells):
                item = QTableWidgetItem(str(val))
                if c == status_col:
                    upper = str(val).upper()
                    if upper == "PASS":
                        item.setForeground(QColor(THEME_COLORS["success"]))
                        item.setFont(QFont("", -1, QFont.Bold))
                    elif upper == "FAIL":
                        item.setForeground(QColor(THEME_COLORS["error"]))
                        item.setFont(QFont("", -1, QFont.Bold))
                    elif upper == "ERROR":
                        item.setForeground(QColor(THEME_COLORS["warning"]))
                        item.setFont(QFont("", -1, QFont.Bold))
                table.setItem(r, c, item)

    def _apply_report_filter(self):
        """Filter the expectation report table by configuration, scenario, protocol, date, and runs."""
        from datetime import datetime as dt, timezone as tz

        selected_config = self._report_config_combo.currentData()
        selected_sid = self._report_filter_combo.currentData()
        selected_proto = self._report_proto_combo.currentData()
        runs_mode = self._report_runs_combo.currentData()
        start_dt = self.from_dt.dateTime().toPyDateTime().replace(tzinfo=tz.utc)
        end_dt = self.to_dt.dateTime().toPyDateTime().replace(tzinfo=tz.utc)

        filtered = self._all_report_rows

        # Configuration name filter
        if selected_config:
            if selected_config == "__unnamed__":
                matching_sids = {sid for sid, info in self._scenario_map.items()
                                 if not info.get("config_name")}
            else:
                matching_sids = {sid for sid, info in self._scenario_map.items()
                                 if info.get("config_name") == selected_config}
            filtered = [row for row in filtered if row["scenario_id"] in matching_sids]

        # Scenario filter
        if selected_sid:
            sid = self._latest_scenario_id if selected_sid == "__latest__" else selected_sid
            filtered = [row for row in filtered if row["scenario_id"] == sid]

        # Protocol filter
        if selected_proto:
            filtered = [row for row in filtered if row["cells"][1] == selected_proto]

        # Date filter
        date_filtered = []
        for row in filtered:
            ts = row.get("timestamp")
            if ts is None:
                date_filtered.append(row)
                continue
            if isinstance(ts, str):
                try:
                    ts = dt.fromisoformat(ts)
                except ValueError:
                    date_filtered.append(row)
                    continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=tz.utc)
            if start_dt <= ts <= end_dt:
                date_filtered.append(row)
        filtered = date_filtered

        # Runs filter: show only the latest run per scenario
        if runs_mode == "latest" and filtered:
            latest_ts = {}
            for row in filtered:
                sid = row["scenario_id"]
                ts = row.get("timestamp")
                if ts is not None:
                    if isinstance(ts, str):
                        try:
                            ts = dt.fromisoformat(ts)
                        except ValueError:
                            continue
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=tz.utc)
                    if sid not in latest_ts or ts > latest_ts[sid]:
                        latest_ts[sid] = ts
            if latest_ts:
                filtered = [row for row in filtered
                            if row["scenario_id"] not in latest_ts or
                            self._parse_ts(row.get("timestamp")) == latest_ts.get(row["scenario_id"])]

        self._populate_report_table(filtered)
        self._update_summary(filtered)

    @staticmethod
    def _parse_ts(ts):
        """Parse a timestamp value to a timezone-aware datetime, or return None."""
        from datetime import datetime as dt, timezone as tz
        if ts is None:
            return None
        if isinstance(ts, str):
            try:
                ts = dt.fromisoformat(ts)
            except ValueError:
                return None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=tz.utc)
        return ts

    def _update_summary(self, rows):
        """Update the Test Summary panel based on the given report rows."""
        status_col = self._report_headers.index("Status")
        pass_count = sum(1 for r in rows if str(r["cells"][status_col]).upper() == "PASS")
        fail_count = sum(1 for r in rows if str(r["cells"][status_col]).upper() == "FAIL")
        error_count = sum(1 for r in rows if str(r["cells"][status_col]).upper() == "ERROR")
        total = pass_count + fail_count + error_count
        if total > 0:
            pass_rate = (pass_count / total) * 100
            if fail_count == 0 and error_count == 0:
                headline = "ALL TESTS PASSED"
                headline_color = THEME_COLORS['success']
            elif error_count > 0 and fail_count == 0:
                headline = f"{error_count} TEST(S) ERRORED"
                headline_color = THEME_COLORS['warning']
            else:
                headline = f"{fail_count} TEST(S) FAILED"
                headline_color = THEME_COLORS['error']
            self.summary_label.setText(
                f"<span style='font-size:22px; font-weight:bold; color:{headline_color};'>"
                f"{headline}</span><br><br>"
                f"<span style='font-size:15px;'>"
                f"Total Expectations: <b>{total}</b> &nbsp;&nbsp;|&nbsp;&nbsp; "
                f"<span style='color:{THEME_COLORS['success']};'>Passed: <b>{pass_count}</b></span> &nbsp;&nbsp;|&nbsp;&nbsp; "
                f"<span style='color:{THEME_COLORS['error']};'>Failed: <b>{fail_count}</b></span> &nbsp;&nbsp;|&nbsp;&nbsp; "
                f"<span style='color:{THEME_COLORS['warning']};'>Errors: <b>{error_count}</b></span> &nbsp;&nbsp;|&nbsp;&nbsp; "
                f"Pass Rate: <b>{pass_rate:.1f}%</b></span>"
            )
        else:
            self.summary_label.setText("No matching results for the selected filters.")

    # ── Export to CSV ──
    def _export_to_csv(self):
        """Export current filtered results to CSV via file dialog."""
        from datetime import timezone as tz

        export_dir = QFileDialog.getExistingDirectory(self, "Select Export Directory")
        if not export_dir:
            return

        scenario_ids = self._scenario_id_order
        if not scenario_ids:
            QMessageBox.warning(self, "No Data", "No results loaded to export. Load results first.")
            return

        # Use date filters if available, otherwise export everything
        if hasattr(self, 'from_dt') and hasattr(self, 'to_dt'):
            start_dt = self.from_dt.dateTime().toPyDateTime().replace(tzinfo=tz.utc)
            end_dt = self.to_dt.dateTime().toPyDateTime().replace(tzinfo=tz.utc)
        else:
            from datetime import datetime
            start_dt = datetime(2000, 1, 1, tzinfo=tz.utc)
            end_dt = datetime.now(tz.utc)

        try:
            from src.utils.db import export_filtered_to_csv
            export_filtered_to_csv(scenario_ids, start_dt, end_dt, export_dir)
            QMessageBox.information(self, "Export Complete", f"Results exported to:\n{export_dir}")
        except Exception as e:
            QMessageBox.critical(self, "Export Failed", f"Failed to export:\n{e}")

    # ── Purge Database ──
    def _nuke_database(self):
        """Stop and remove the Postgres container and its volume."""
        reply = QMessageBox.warning(
            self, "Purge Database",
            "This will permanently destroy the Postgres database by stopping "
            "and removing the Docker container and the 'load-test' volume.\n\n"
            "All unsaved results will be lost. Make sure you have exported "
            "the results before proceeding.\n\n"
            "Are you sure?",
            QMessageBox.Yes | QMessageBox.Cancel,
            QMessageBox.Cancel,
        )
        if reply != QMessageBox.Yes:
            return

        self._nuke_btn.setEnabled(False)

        self._nuke_thread = QThread()
        self._nuke_worker = _NukeWorker()
        self._nuke_worker.moveToThread(self._nuke_thread)

        self._nuke_thread.started.connect(self._nuke_worker.run)
        self._nuke_worker.done.connect(self._on_nuke_done)
        self._nuke_worker.finished.connect(self._nuke_thread.quit)
        self._nuke_worker.finished.connect(lambda: self._nuke_btn.setEnabled(True))

        self._nuke_thread.start()

    def _on_nuke_done(self, errors):
        if errors:
            QMessageBox.warning(self, "Purge Database", "Completed with errors:\n" + "\n".join(errors))
        else:
            QMessageBox.information(self, "Purge Database", "Database container and volume removed successfully.")


# ---------------------------------------------------------------------------
# Main Window
# ---------------------------------------------------------------------------
class MainWindow(QMainWindow):
    """Main application window."""

    def __init__(self):
        super().__init__()
        self.setAttribute(Qt.WA_OpaquePaintEvent)
        self.setWindowTitle("Load Test Automation Framework")
        self.setMinimumWidth(1280)
        self.setMinimumHeight(860)
        self._build_ui()
        self._load_default_config()

    def resizeEvent(self, event):
        if self.width() < 1280 or self.height() < 860:
            self.resize(max(self.width(), 1280), max(self.height(), 860))
        self._bg_cache = None
        super().resizeEvent(event)

    def paintEvent(self, event):
        painter = QPainter(self)
        size = self.size()
        cache = getattr(self, '_bg_cache', None)
        if cache is None or cache.size() != size:
            bg = QPixmap(size)
            p = QPainter(bg)
            gradient = QLinearGradient(0, 0, size.width(), size.height())
            gradient.setColorAt(0.0, QColor("#080812"))
            gradient.setColorAt(0.15, QColor("#06060e"))
            gradient.setColorAt(0.35, QColor("#04040a"))
            gradient.setColorAt(0.5, QColor(THEME_COLORS['bg_dark']))
            gradient.setColorAt(0.65, QColor("#04040a"))
            gradient.setColorAt(0.85, QColor("#07050e"))
            gradient.setColorAt(1.0, QColor("#0a0810"))
            p.fillRect(bg.rect(), gradient)
            p.end()
            self._bg_cache = bg
        painter.drawPixmap(0, 0, self._bg_cache)
        painter.end()

    def _build_ui(self):
        # Title bar / toolbar
        toolbar = QToolBar()
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(20, 20))

        logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo", "honeywell-logo-11530965197um5hebvsd4.png")
        title_label = QLabel()
        title_label.setAttribute(Qt.WA_TranslucentBackground)
        logo_pixmap = build_toolbar_logo_pixmap(logo_path, target_height=28)
        title_label.setPixmap(logo_pixmap)
        title_label.setStyleSheet("background: transparent; border: none; padding: 4px 2px;")
        toolbar.addWidget(title_label)

        # Separator dot
        sep = QLabel("\u2022")
        sep.setStyleSheet(
            f"color: {THEME_COLORS['border_light']}; font-size: 18px; "
            f"background: transparent; padding: 0 4px;"
        )
        toolbar.addWidget(sep)

        subtitle = QLabel("Load Test Automation Framework")
        subtitle.setStyleSheet(
            f"font-size: 20px; color: #f0f0f4; "
            f"background: transparent; padding-left: 4px; letter-spacing: 1.5px;"
            f"font-weight: bold; text-transform: uppercase;"
        )
        toolbar.addWidget(subtitle)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        spacer.setStyleSheet("background: transparent;")
        toolbar.addWidget(spacer)

        self.addToolBar(toolbar)

        # Central widget with tabs
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)

        _icon_color = THEME_COLORS['text_secondary']

        self.config_tab = ConfigurationTab()
        self.tabs.addTab(self.config_tab, qta.icon("fa5s.cogs", color=_icon_color), "  Configuration  ")

        self.runner_tab = TestRunnerTab(self.config_tab)
        self.tabs.addTab(self.runner_tab, qta.icon("fa5s.play-circle", color=_icon_color), "  Run Tests  ")

        self.results_tab = ResultsTab(config_tab=self.config_tab)
        self.tabs.addTab(self.results_tab, qta.icon("fa5s.chart-bar", color=_icon_color), "  Results  ")

        self.setCentralWidget(self.tabs)

    def _load_default_config(self):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configurations", "main.json")
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)
                self.config_tab.load_config_data(config)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)
    app = QApplication(sys.argv)
    app.setApplicationName("Load Test Framework")
    app.setDesktopFileName("loadtestframework")
    app.setStyle("Fusion")
    app.setStyleSheet(STYLESHEET)

    # Set ultra-dark palette as base
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(THEME_COLORS["bg_dark"]))
    palette.setColor(QPalette.WindowText, QColor(THEME_COLORS["text_primary"]))
    palette.setColor(QPalette.Base, QColor(THEME_COLORS["bg_card"]))
    palette.setColor(QPalette.AlternateBase, QColor(THEME_COLORS["table_alt_row"]))
    palette.setColor(QPalette.Text, QColor(THEME_COLORS["text_primary"]))
    palette.setColor(QPalette.Button, QColor(THEME_COLORS["bg_light"]))
    palette.setColor(QPalette.ButtonText, QColor(THEME_COLORS["text_primary"]))
    palette.setColor(QPalette.Highlight, QColor(THEME_COLORS["red_primary"]))
    palette.setColor(QPalette.HighlightedText, QColor("white"))
    palette.setColor(QPalette.ToolTipBase, QColor(THEME_COLORS["bg_elevated"]))
    palette.setColor(QPalette.ToolTipText, QColor(THEME_COLORS["text_primary"]))
    palette.setColor(QPalette.PlaceholderText, QColor(THEME_COLORS["text_muted"]))
    app.setPalette(palette)

    window = MainWindow()
    window.show()
    # Defer maximize until the event loop is running so the window manager
    # sees the window mapped promptly (avoids "not responding" on slow starts).
    QTimer.singleShot(0, window.showMaximized)
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
