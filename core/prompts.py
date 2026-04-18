"""
Prompts — research prompt split into 2 chunks (IDs 1-82, 83-163)
to avoid token truncation. Consolidation prompt picks the best value per key.
"""
import json

# ── Full schema definition ────────────────────────────────────────────────────
_SCHEMA_ROWS = [
    (1,   "Company Basics",          "Company Name",                     "Atomic"),
    (2,   "Company Basics",          "Short Name",                       "Atomic"),
    (3,   "Company Basics",          "Logo URL",                         "Composite"),
    (4,   "Company Basics",          "Category",                         "Atomic"),
    (5,   "Company Basics",          "Year of Incorporation",            "Atomic"),
    (6,   "Company Narrative",       "Overview of the Company",          "Atomic"),
    (7,   "Company Basics",          "Nature of Company",                "Atomic"),
    (8,   "Company Basics",          "Company Headquarters",             "Atomic"),
    (9,   "Geographic Presence",     "Countries Operating In",           "Composite"),
    (10,  "Geographic Presence",     "Number of Offices",                "Atomic"),
    (11,  "Geographic Presence",     "Office Locations",                 "Composite"),
    (12,  "People & Talent",         "Employee Size",                    "Atomic"),
    (13,  "People & Talent",         "Hiring Velocity",                  "Composite"),
    (14,  "People & Talent",         "Employee Turnover",                "Atomic"),
    (15,  "People & Talent",         "Average Retention Tenure",         "Atomic"),
    (16,  "Business Model",          "Pain Points Being Addressed",      "Composite"),
    (17,  "Business Model",          "Focus Sectors / Industries",       "Composite"),
    (18,  "Business Model",          "Services / Offerings / Products",  "Composite"),
    (19,  "Business Model",          "Top Customers by Client Segments", "Composite"),
    (20,  "Business Model",          "Core Value Proposition",           "Composite"),
    (21,  "Strategy & Culture",      "Vision",                           "Atomic"),
    (22,  "Strategy & Culture",      "Mission",                          "Atomic"),
    (23,  "Strategy & Culture",      "Values",                           "Composite"),
    (24,  "Strategy & Culture",      "Unique Differentiators",           "Composite"),
    (25,  "Strategy & Culture",      "Competitive Advantages",           "Composite"),
    (26,  "Strategy & Culture",      "Weaknesses / Gaps in Offering",    "Composite"),
    (27,  "Strategy & Culture",      "Key Challenges and Unmet Needs",   "Composite"),
    (28,  "Competitive Landscape",   "Key Competitors",                  "Composite"),
    (29,  "Competitive Landscape",   "Technology Partners",              "Composite"),
    (30,  "Company Narrative",       "Interesting Facts",                "Composite"),
    (31,  "Company Narrative",       "Recent News",                      "Composite"),
    (32,  "Digital Presence",        "Website URL",                      "Atomic"),
    (33,  "Digital Presence",        "Quality of Website",               "Atomic"),
    (34,  "Digital Presence",        "Website Rating",                   "Atomic"),
    (35,  "Digital Presence",        "Website Traffic Rank",             "Composite"),
    (36,  "Digital Presence",        "Social Media Followers Combined",  "Atomic"),
    (37,  "Digital Presence",        "Glassdoor Rating",                 "Atomic"),
    (38,  "Digital Presence",        "Indeed Rating",                    "Atomic"),
    (39,  "Digital Presence",        "Google Reviews Rating",            "Atomic"),
    (40,  "Digital Presence",        "LinkedIn Profile URL",             "Atomic"),
    (41,  "Digital Presence",        "Twitter X Handle",                 "Atomic"),
    (42,  "Digital Presence",        "Facebook Page URL",                "Atomic"),
    (43,  "Digital Presence",        "Instagram Page URL",               "Atomic"),
    (44,  "Leadership",              "CEO Name",                         "Atomic"),
    (45,  "Leadership",              "CEO LinkedIn URL",                 "Atomic"),
    (46,  "Leadership",              "Key Business Leaders",             "Composite"),
    (47,  "Leadership",              "Warm Introduction Pathways",       "Composite"),
    (48,  "Leadership",              "Decision Maker Accessibility",     "Atomic"),
    (49,  "Contact Info",            "Company Contact Email",            "Atomic"),
    (50,  "Contact Info",            "Company Phone Number",             "Atomic"),
    (51,  "Contact Info",            "Primary Contact Name",             "Atomic"),
    (52,  "Contact Info",            "Primary Contact Title",            "Atomic"),
    (53,  "Contact Info",            "Primary Contact Email",            "Atomic"),
    (54,  "Contact Info",            "Primary Contact Phone",            "Atomic"),
    (55,  "Reputation",              "Awards and Recognitions",          "Composite"),
    (56,  "Reputation",              "Brand Sentiment Score",            "Atomic"),
    (57,  "Reputation",              "Event Participation",              "Composite"),
    (58,  "Risk & Compliance",       "Regulatory Compliance Status",     "Composite"),
    (59,  "Risk & Compliance",       "Legal Issues Controversies",       "Atomic"),
    (60,  "Financials",              "Annual Revenues",                  "Atomic"),
    (61,  "Financials",              "Annual Profits",                   "Atomic"),
    (62,  "Financials",              "Revenue Mix",                      "Composite"),
    (63,  "Financials",              "Company Valuation",                "Atomic"),
    (64,  "Financials",              "Year-over-Year Growth Rate",       "Atomic"),
    (65,  "Financials",              "Profitability Status",             "Atomic"),
    (66,  "Financials",              "Market Share",                     "Atomic"),
    (67,  "Funding",                 "Key Investors Backers",            "Composite"),
    (68,  "Funding",                 "Recent Funding Rounds",            "Composite"),
    (69,  "Funding",                 "Total Capital Raised",             "Atomic"),
    (70,  "Sustainability",          "ESG Practices or Ratings",         "Composite"),
    (71,  "Sales & Growth",          "Sales Motion",                     "Atomic"),
    (72,  "Sales & Growth",          "Customer Acquisition Cost",        "Atomic"),
    (73,  "Sales & Growth",          "Customer Lifetime Value",          "Atomic"),
    (74,  "Sales & Growth",          "CAC LTV Ratio",                    "Atomic"),
    (75,  "Sales & Growth",          "Churn Rate",                       "Atomic"),
    (76,  "Sales & Growth",          "Net Promoter Score",               "Atomic"),
    (77,  "Sales & Growth",          "Customer Concentration Risk",      "Atomic"),
    (78,  "Sales & Growth",          "Burn Rate",                        "Atomic"),
    (79,  "Sales & Growth",          "Runway",                           "Atomic"),
    (80,  "Sales & Growth",          "Burn Multiplier",                  "Atomic"),
    (81,  "Innovation",              "Intellectual Property",            "Composite"),
    (82,  "Innovation",              "R&D Investment",                   "Atomic"),
    (83,  "Innovation",              "AI ML Adoption Level",             "Atomic"),
    (84,  "Operations",              "Tech Stack Tools Used",            "Composite"),
    (85,  "Operations",              "Cybersecurity Posture",            "Composite"),
    (86,  "Operations",              "Supply Chain Dependencies",        "Composite"),
    (87,  "Operations",              "Geopolitical Risks",               "Composite"),
    (88,  "Operations",              "Macro Risks",                      "Composite"),
    (89,  "People & Talent",         "Diversity Metrics",                "Composite"),
    (90,  "People & Talent",         "Remote Work Policy",               "Atomic"),
    (91,  "People & Talent",         "Training Development Spend",       "Atomic"),
    (92,  "Market",                  "Partnership Ecosystem",            "Composite"),
    (93,  "Market",                  "Exit Strategy History",            "Composite"),
    (94,  "Sustainability",          "Carbon Footprint",                 "Atomic"),
    (95,  "Sustainability",          "Ethical Sourcing Practices",       "Composite"),
    (96,  "Benchmarking",            "Benchmark vs Peers",               "Composite"),
    (97,  "Forecasting",             "Future Projections",               "Atomic"),
    (98,  "Forecasting",             "Strategic Priorities",             "Composite"),
    (99,  "Network",                 "Industry Associations",            "Composite"),
    (100, "Proof Points",            "Case Studies",                     "Composite"),
    (101, "Go-to-Market",            "Go-to-Market Strategy",            "Composite"),
    (102, "Innovation",              "Innovation Roadmap",               "Composite"),
    (103, "Innovation",              "Product Pipeline",                 "Composite"),
    (104, "Governance",              "Board of Directors Advisors",      "Composite"),
    (105, "Digital Presence",        "Company Marketing Videos",         "Composite"),
    (106, "Proof Points",            "Customer Testimonials",            "Composite"),
    (107, "Benchmarking",            "Industry Benchmark Tech Adoption", "Composite"),
    (108, "Market",                  "Total Addressable Market TAM",     "Atomic"),
    (109, "Market",                  "Serviceable Addressable Market",   "Atomic"),
    (110, "Market",                  "Serviceable Obtainable Market",    "Atomic"),
    (111, "Culture & People",        "Work Culture",                     "Composite"),
    (112, "Culture & People",        "Manager Quality",                  "Atomic"),
    (113, "Culture & People",        "Psychological Safety",             "Atomic"),
    (114, "Culture & People",        "Feedback Culture",                 "Composite"),
    (115, "Culture & People",        "Diversity Inclusion",              "Composite"),
    (116, "Culture & People",        "Ethical Standards",                "Composite"),
    (117, "Work-Life Balance",       "Typical Working Hours",            "Atomic"),
    (118, "Work-Life Balance",       "Overtime Expectations",            "Atomic"),
    (119, "Work-Life Balance",       "Weekend Work",                     "Atomic"),
    (120, "Work-Life Balance",       "Remote Hybrid Flexibility",        "Composite"),
    (121, "Work-Life Balance",       "Leave Policy",                     "Composite"),
    (122, "Work-Life Balance",       "Burnout Risk",                     "Atomic"),
    (123, "Location & Accessibility","Central vs Peripheral Location",   "Atomic"),
    (124, "Location & Accessibility","Public Transport Access",          "Composite"),
    (125, "Location & Accessibility","Cab Availability Policy",          "Composite"),
    (126, "Location & Accessibility","Commute Time from Airport",        "Atomic"),
    (127, "Location & Accessibility","Office Zone Type",                 "Atomic"),
    (128, "Safety & Well-being",     "Area Safety",                      "Composite"),
    (129, "Safety & Well-being",     "Company Safety Policies",          "Composite"),
    (130, "Safety & Well-being",     "Office Infrastructure Safety",     "Composite"),
    (131, "Safety & Well-being",     "Emergency Response Preparedness",  "Composite"),
    (132, "Safety & Well-being",     "Health Support",                   "Composite"),
    (133, "Learning & Growth",       "Onboarding Training Quality",      "Atomic"),
    (134, "Learning & Growth",       "Learning Culture",                 "Composite"),
    (135, "Learning & Growth",       "Exposure Quality",                 "Atomic"),
    (136, "Learning & Growth",       "Mentorship Availability",          "Composite"),
    (137, "Learning & Growth",       "Internal Mobility",                "Atomic"),
    (138, "Learning & Growth",       "Promotion Clarity",                "Composite"),
    (139, "Learning & Growth",       "Tools Technology Access",          "Composite"),
    (140, "Role & Work Quality",     "Role Clarity",                     "Atomic"),
    (141, "Role & Work Quality",     "Early Ownership",                  "Atomic"),
    (142, "Role & Work Quality",     "Work Impact",                      "Composite"),
    (143, "Role & Work Quality",     "Execution vs Thinking Balance",    "Atomic"),
    (144, "Role & Work Quality",     "Automation Level",                 "Atomic"),
    (145, "Role & Work Quality",     "Cross-functional Exposure",        "Composite"),
    (146, "Company Stability",       "Company Maturity",                 "Atomic"),
    (147, "Company Stability",       "Brand Value",                      "Atomic"),
    (148, "Company Stability",       "Client Quality",                   "Composite"),
    (149, "Company Stability",       "Layoff History",                   "Atomic"),
    (150, "Compensation & Benefits", "Fixed vs Variable Pay",            "Atomic"),
    (151, "Compensation & Benefits", "Bonus Predictability",             "Atomic"),
    (152, "Compensation & Benefits", "ESOPs Long-term Incentives",       "Composite"),
    (153, "Compensation & Benefits", "Family Health Insurance",          "Composite"),
    (154, "Compensation & Benefits", "Relocation Support",               "Composite"),
    (155, "Compensation & Benefits", "Lifestyle Wellness Benefits",      "Composite"),
    (156, "Long-Term Career",        "Exit Opportunities",               "Composite"),
    (157, "Long-Term Career",        "Skill Relevance",                  "Atomic"),
    (158, "Long-Term Career",        "External Recognition",             "Atomic"),
    (159, "Long-Term Career",        "Network Strength",                 "Composite"),
    (160, "Long-Term Career",        "Global Exposure",                  "Composite"),
    (161, "Values Alignment",        "Mission Clarity",                  "Atomic"),
    (162, "Values Alignment",        "Sustainability and CSR",           "Composite"),
    (163, "Values Alignment",        "Crisis Behavior",                  "Atomic"),
]

# ── Keep old array-style instructions (used internally by agent2 candidates) ──
_INSTRUCTIONS = """RULES:
- Return ONLY a valid JSON array. No markdown, no explanation, no extra text.
- Each object must have EXACTLY these 5 keys:
    "ID"                    → the integer ID number from the schema
    "Category"              → the category string from the schema (copy exactly)
    "A/C"                   → MUST be exactly the word "Atomic" or the word "Composite" (copy from schema column A/C)
    "Parameter"             → the parameter name from the schema (copy exactly)
    "Research Output / Data"→ your researched answer for this parameter
- CRITICAL: "A/C" must ONLY contain "Atomic" or "Composite". Never put company name or any other value there.
- Atomic field → single value string in "Research Output / Data".
- Composite field → multiple values separated by semicolons e.g. "Val1;Val2;Val3" in "Research Output / Data".
- Never leave blank. Use "Not Found" only if truly unavailable.
- Output exactly {count} objects — one per ID listed."""

# ── Flat key map: ID → snake_case key ─────────────────────────────────────────
_FLAT_KEYS: dict[int, str] = {
    1:   "company_name",
    2:   "short_name",
    3:   "logo_url",
    4:   "category",
    5:   "year_of_incorporation",
    6:   "overview_of_the_company",
    7:   "nature_of_company",
    8:   "company_headquarters",
    9:   "countries_operating_in",
    10:  "number_of_offices",
    11:  "office_locations",
    12:  "employee_size",
    13:  "hiring_velocity",
    14:  "employee_turnover",
    15:  "average_retention_tenure",
    16:  "pain_points_being_addressed",
    17:  "focus_sectors_industries",
    18:  "services_offerings_products",
    19:  "top_customers_by_client_segments",
    20:  "core_value_proposition",
    21:  "vision",
    22:  "mission",
    23:  "values",
    24:  "unique_differentiators",
    25:  "competitive_advantages",
    26:  "weaknesses_gaps_in_offering",
    27:  "key_challenges_and_unmet_needs",
    28:  "key_competitors",
    29:  "technology_partners",
    30:  "interesting_facts",
    31:  "recent_news",
    32:  "website_url",
    33:  "quality_of_website",
    34:  "website_rating",
    35:  "website_traffic_rank",
    36:  "social_media_followers_combined",
    37:  "glassdoor_rating",
    38:  "indeed_rating",
    39:  "google_reviews_rating",
    40:  "linkedin_profile_url",
    41:  "twitter_x_handle",
    42:  "facebook_page_url",
    43:  "instagram_page_url",
    44:  "ceo_name",
    45:  "ceo_linkedin_url",
    46:  "key_business_leaders",
    47:  "warm_introduction_pathways",
    48:  "decision_maker_accessibility",
    49:  "company_contact_email",
    50:  "company_phone_number",
    51:  "primary_contact_name",
    52:  "primary_contact_title",
    53:  "primary_contact_email",
    54:  "primary_contact_phone",
    55:  "awards_and_recognitions",
    56:  "brand_sentiment_score",
    57:  "event_participation",
    58:  "regulatory_compliance_status",
    59:  "legal_issues_controversies",
    60:  "annual_revenues",
    61:  "annual_profits",
    62:  "revenue_mix",
    63:  "company_valuation",
    64:  "year_over_year_growth_rate",
    65:  "profitability_status",
    66:  "market_share",
    67:  "key_investors_backers",
    68:  "recent_funding_rounds",
    69:  "total_capital_raised",
    70:  "esg_practices_or_ratings",
    71:  "sales_motion",
    72:  "customer_acquisition_cost",
    73:  "customer_lifetime_value",
    74:  "cac_ltv_ratio",
    75:  "churn_rate",
    76:  "net_promoter_score",
    77:  "customer_concentration_risk",
    78:  "burn_rate",
    79:  "runway",
    80:  "burn_multiplier",
    81:  "intellectual_property",
    82:  "rd_investment",
    83:  "ai_ml_adoption_level",
    84:  "tech_stack_tools_used",
    85:  "cybersecurity_posture",
    86:  "supply_chain_dependencies",
    87:  "geopolitical_risks",
    88:  "macro_risks",
    89:  "diversity_metrics",
    90:  "remote_work_policy",
    91:  "training_development_spend",
    92:  "partnership_ecosystem",
    93:  "exit_strategy_history",
    94:  "carbon_footprint",
    95:  "ethical_sourcing_practices",
    96:  "benchmark_vs_peers",
    97:  "future_projections",
    98:  "strategic_priorities",
    99:  "industry_associations",
    100: "case_studies",
    101: "go_to_market_strategy",
    102: "innovation_roadmap",
    103: "product_pipeline",
    104: "board_of_directors_advisors",
    105: "company_marketing_videos",
    106: "customer_testimonials",
    107: "industry_benchmark_tech_adoption",
    108: "total_addressable_market_tam",
    109: "serviceable_addressable_market",
    110: "serviceable_obtainable_market",
    111: "work_culture",
    112: "manager_quality",
    113: "psychological_safety",
    114: "feedback_culture",
    115: "diversity_inclusion",
    116: "ethical_standards",
    117: "typical_working_hours",
    118: "overtime_expectations",
    119: "weekend_work",
    120: "remote_hybrid_flexibility",
    121: "leave_policy",
    122: "burnout_risk",
    123: "central_vs_peripheral_location",
    124: "public_transport_access",
    125: "cab_availability_policy",
    126: "commute_time_from_airport",
    127: "office_zone_type",
    128: "area_safety",
    129: "company_safety_policies",
    130: "office_infrastructure_safety",
    131: "emergency_response_preparedness",
    132: "health_support",
    133: "onboarding_training_quality",
    134: "learning_culture",
    135: "exposure_quality",
    136: "mentorship_availability",
    137: "internal_mobility",
    138: "promotion_clarity",
    139: "tools_technology_access",
    140: "role_clarity",
    141: "early_ownership",
    142: "work_impact",
    143: "execution_vs_thinking_balance",
    144: "automation_level",
    145: "cross_functional_exposure",
    146: "company_maturity",
    147: "brand_value",
    148: "client_quality",
    149: "layoff_history",
    150: "fixed_vs_variable_pay",
    151: "bonus_predictability",
    152: "esops_long_term_incentives",
    153: "family_health_insurance",
    154: "relocation_support",
    155: "lifestyle_wellness_benefits",
    156: "exit_opportunities",
    157: "skill_relevance",
    158: "external_recognition",
    159: "network_strength",
    160: "global_exposure",
    161: "mission_clarity",
    162: "sustainability_and_csr",
    163: "crisis_behavior",
}

# Reverse map: snake_key → ID  (used when parsing flat-object responses)
_FLAT_KEY_TO_ID: dict[str, int] = {v: k for k, v in _FLAT_KEYS.items()}

_SCHEMA_BY_ID = {row[0]: row for row in _SCHEMA_ROWS}


def _build_schema_table(rows: list) -> str:
    lines = ["ID | Category | Parameter | A/C"]
    for id_, cat, param, ac in rows:
        lines.append(f"{id_} | {cat} | {param} | {ac}")
    return "\n".join(lines)


def _build_flat_schema_lines(rows: list) -> str:
    """
    Schema reference for flat-object prompts.
    Format: key | description | Atomic/Composite
    """
    lines = ["key | description | type"]
    for id_, cat, param, ac in rows:
        key = _FLAT_KEYS[id_]
        lines.append(f"{key} | {param} ({cat}) | {ac}")
    return "\n".join(lines)


def build_research_prompt(company_name: str, chunk: int) -> str:
    """
    Agent 1 research prompt — returns a FLAT JSON OBJECT (not an array).

    chunk=1  IDs 1-82
    chunk=2  IDs 83-163

    Output format (single object, keys only for this chunk):
    {
      "company_name": "Stripe",
      "short_name": "Stripe",
      ...
    }
    """
    if chunk == 1:
        rows = [r for r in _SCHEMA_ROWS if r[0] <= 82]
    else:
        rows = [r for r in _SCHEMA_ROWS if r[0] > 82]

    start_id = rows[0][0]
    end_id   = rows[-1][0]
    schema   = _build_flat_schema_lines(rows)

    # Build the expected key list so the LLM knows exactly what to output
    expected_keys = "\n".join(f'  "{_FLAT_KEYS[r[0]]}"' for r in rows)

    return f"""You are a corporate research analyst. Research "{company_name}" and return data for fields {start_id}-{end_id}.

RULES:
- Return ONLY a single valid JSON object. No markdown. No explanation. No extra text.
- Do NOT return an array. Do NOT wrap in any outer key.
- The object must contain EXACTLY these {len(rows)} keys (and no others):
{expected_keys}
- Atomic field  → single string value.
- Composite field → multiple values as one string separated by semicolons e.g. "Val1;Val2;Val3".
- Never leave a value blank. Use "Not Found" only if truly unavailable.
- Start your response with {{ and end with }}

SCHEMA (key | description | type):
{schema}

Return the JSON object now."""


def build_targeted_research_prompt(company_name: str, field_ids: list[int]) -> str:
    """
    Remediation prompt — flat object for only the failed field IDs.
    """
    ids_set = set()
    for raw in field_ids:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value in _SCHEMA_BY_ID:
            ids_set.add(value)
    ids = sorted(ids_set)
    if not ids:
        raise ValueError("No valid field IDs provided for targeted research prompt")

    rows = [_SCHEMA_BY_ID[i] for i in ids]
    schema = _build_flat_schema_lines(rows)
    expected_keys = "\n".join(f'  "{_FLAT_KEYS[r[0]]}"' for r in rows)
    id_list = ", ".join(str(i) for i in ids)

    return f"""You are a corporate research analyst. Research "{company_name}" and return data ONLY for these fields: {id_list}.

RULES:
- Return ONLY a single valid JSON object. No markdown. No explanation. No extra text.
- Do NOT return an array. Do NOT wrap in any outer key.
- The object must contain EXACTLY these {len(rows)} keys (and no others):
{expected_keys}
- Atomic field  → single string value.
- Composite field → multiple values as one string separated by semicolons e.g. "Val1;Val2;Val3".
- Never leave a value blank. Use "Not Found" only if truly unavailable.
- Start your response with {{ and end with }}

SCHEMA (key | description | type):
{schema}

Return the JSON object now."""


def build_consolidation_prompt(candidates: list) -> str:
    """
    Agent 2 consolidation prompt.
    Receives up to 3 candidate flat-objects per chunk (one per LLM source).
    Must output ONE merged flat object picking the best value per key.
    """
    # Determine which keys are present across all candidates
    all_keys = set()
    for obj in candidates:
        if isinstance(obj, dict):
            all_keys.update(obj.keys())
    # Filter to only valid schema keys, keep ordering by ID
    ordered_keys = [_FLAT_KEYS[i] for i in range(1, 164) if _FLAT_KEYS[i] in all_keys]

    expected_keys = "\n".join(f'  "{k}"' for k in ordered_keys)

    return f"""You are a data quality engine. You have {len(candidates)} candidate JSON objects from different LLMs for the same company.
Each object has the same keys but different values. Pick the single best value per key.

PRIORITY: Reliability > Completeness. A truthful "Not Found" beats a fabricated value.

RULES:
- Return ONLY a single valid JSON object. No markdown. No explanation.
- The object must contain EXACTLY these keys:
{expected_keys}
- Never invent or merge values — pick one existing candidate value exactly as-is.
- Prefer values that are specific and consistent across multiple sources.
- If candidates conflict with low confidence, use "Not Found".
- Treat obvious placeholders as low quality (e.g. "John Smith", "test@example.com").
- For contact fields (primary_contact_*), do NOT prefer a placeholder over "Not Found".
- Start your response with {{ and end with }}

CANDIDATES:
{json.dumps(candidates, indent=2)}

Return the consolidated JSON object now."""
