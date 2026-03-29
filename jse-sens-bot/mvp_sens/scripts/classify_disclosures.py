from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

CATEGORY_TRADING_STATEMENT = "trading_statement"
CATEGORY_FINANCIAL_RESULTS = "financial_results"
CATEGORY_EARNINGS_UPDATE = "earnings_update"
CATEGORY_OTHER = "other"

CLASSIFICATION_VERSION = "phase2-r2-v1"

ISSUER_TAG_EQ = "equity"
ISSUER_TAG_INTEREST_RATE = "interest_rate"
ISSUER_TAG_ETF = "etf"
ISSUER_TAG_ETN = "etn"
ISSUER_TAG_HYBRID = "hybrid"

ISSUER_TEXT_MAP = (
    ("actively managed etf issuer", ISSUER_TAG_ETF),
    ("interest rate issuer", ISSUER_TAG_INTEREST_RATE),
    ("equity issuer", ISSUER_TAG_EQ),
    ("etf issuer", ISSUER_TAG_ETF),
    ("etn issuer", ISSUER_TAG_ETN),
    ("hybrid issuer", ISSUER_TAG_HYBRID),
)

TRADING_PATTERNS = (
    ("trading statement", "kw_trading_statement"),
)

FINANCIAL_PATTERNS = (
    ("financial results", "kw_financial_results"),
    ("financial statements", "kw_financial_statements"),
    ("annual results", "kw_annual_results"),
    ("interim results", "kw_interim_results"),
    ("results for the year ended", "kw_results_year_ended"),
    ("results for the six months ended", "kw_results_six_months"),
    ("condensed consolidated", "kw_condensed_consolidated"),
)

EARNINGS_PATTERNS = (
    ("headline earnings", "kw_headline_earnings"),
    ("earnings update", "kw_earnings_update"),
    ("heps", "kw_heps"),
    ("eps", "kw_eps"),
    ("earnings", "kw_earnings"),
)

ANNUAL_REPORT_PATTERNS = (
    ("annual report", "kw_annual_report"),
)

EXCLUDE_PATTERNS = (
    ("beneficial ownership", "excluded_beneficial_ownership"),
    ("major holdings", "excluded_major_holdings"),
    ("dealings in securities", "excluded_dealings_in_securities"),
    ("dealing in securities", "excluded_dealing_in_securities"),
    ("dealing by security by related party", "excluded_related_party_dealing"),
    ("director declaration", "excluded_director_declaration"),
    ("change in directorate", "excluded_change_in_directorate"),
    ("listing of additional", "excluded_listing_of_additional"),
    ("partial de-listing", "excluded_partial_delisting"),
    ("partial redemption", "excluded_partial_redemption"),
    ("interest payment notifications", "excluded_interest_payment_notification"),
    ("interest payments", "excluded_interest_payments"),
    ("notification of issue", "excluded_notification_of_issue"),
    ("results of annual general meeting", "excluded_agm_results"),
    ("results of general meeting", "excluded_general_meeting_results"),
)


@dataclass(frozen=True)
class ClassificationResult:
    category: str
    classification_reason: str
    analyst_relevant: bool
    relevance_reason: str
    issuer_tags: tuple[str, ...]
    issuer_allowed: bool
    issuer_reason: str
    ambiguous: bool


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.lower().split())


def _first_pattern_hit(
    normalized_text: str,
    patterns: Iterable[tuple[str, str]],
) -> tuple[str, str] | None:
    for pattern, rule_id in patterns:
        if pattern in normalized_text:
            return pattern, rule_id
    return None


def extract_issuer_tags(*texts: str) -> tuple[str, ...]:
    normalized = " ".join(_normalize_text(text) for text in texts if text).strip()
    if not normalized:
        return ()

    tags: list[str] = []
    for issuer_text, issuer_tag in ISSUER_TEXT_MAP:
        if issuer_text in normalized and issuer_tag not in tags:
            tags.append(issuer_tag)
    return tuple(tags)


def evaluate_issuer_eligibility(*texts: str) -> tuple[bool, str, tuple[str, ...]]:
    tags = extract_issuer_tags(*texts)
    if not tags:
        return False, "issuer_unknown", tags
    if ISSUER_TAG_EQ in tags:
        return True, "issuer_equity", tags
    return False, "issuer_non_equity", tags


def classify_announcement(
    title: str,
    issuer_context: str = "",
    body_text: str = "",
) -> ClassificationResult:
    normalized_title = _normalize_text(title)
    issuer_allowed, issuer_reason, issuer_tags = evaluate_issuer_eligibility(
        title,
        issuer_context,
    )

    trading_hit = _first_pattern_hit(normalized_title, TRADING_PATTERNS)
    financial_hit = _first_pattern_hit(normalized_title, FINANCIAL_PATTERNS)
    earnings_hit = _first_pattern_hit(normalized_title, EARNINGS_PATTERNS)
    annual_report_hit = _first_pattern_hit(normalized_title, ANNUAL_REPORT_PATTERNS)
    exclude_hit = _first_pattern_hit(normalized_title, EXCLUDE_PATTERNS)

    category = CATEGORY_OTHER
    classification_reason = "category_other_default"

    if trading_hit is not None:
        category = CATEGORY_TRADING_STATEMENT
        classification_reason = trading_hit[1]
    elif financial_hit is not None:
        category = CATEGORY_FINANCIAL_RESULTS
        classification_reason = financial_hit[1]
    elif earnings_hit is not None:
        category = CATEGORY_EARNINGS_UPDATE
        classification_reason = earnings_hit[1]

    analyst_relevant = category != CATEGORY_OTHER
    relevance_reason = classification_reason if analyst_relevant else "relevance_other_default"

    # "Annual Report and Notice of AGM" remains relevant by explicit policy.
    if annual_report_hit is not None:
        analyst_relevant = True
        if category == CATEGORY_OTHER:
            relevance_reason = annual_report_hit[1]

    if exclude_hit is not None and annual_report_hit is None and category == CATEGORY_OTHER:
        analyst_relevant = False
        relevance_reason = exclude_hit[1]

    if not issuer_allowed:
        analyst_relevant = False
        relevance_reason = issuer_reason

    ambiguous = False
    if category == CATEGORY_OTHER and annual_report_hit is None:
        if "statement" in normalized_title or "results" in normalized_title:
            ambiguous = True

    if category == CATEGORY_OTHER and body_text:
        normalized_body = _normalize_text(body_text)
        body_trading = _first_pattern_hit(normalized_body, TRADING_PATTERNS)
        body_financial = _first_pattern_hit(normalized_body, FINANCIAL_PATTERNS)
        body_earnings = _first_pattern_hit(normalized_body, EARNINGS_PATTERNS)

        if body_trading is not None:
            category = CATEGORY_TRADING_STATEMENT
            classification_reason = f"pdf_{body_trading[1]}"
        elif body_financial is not None:
            category = CATEGORY_FINANCIAL_RESULTS
            classification_reason = f"pdf_{body_financial[1]}"
        elif body_earnings is not None:
            category = CATEGORY_EARNINGS_UPDATE
            classification_reason = f"pdf_{body_earnings[1]}"

        if category != CATEGORY_OTHER:
            analyst_relevant = True
            relevance_reason = classification_reason
            ambiguous = False

    return ClassificationResult(
        category=category,
        classification_reason=classification_reason,
        analyst_relevant=analyst_relevant,
        relevance_reason=relevance_reason,
        issuer_tags=issuer_tags,
        issuer_allowed=issuer_allowed,
        issuer_reason=issuer_reason,
        ambiguous=ambiguous,
    )
