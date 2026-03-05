"""
Pydantic Models — 163-field validation
All 163 parameters registered with ID, Category, A/C, Parameter.
validate_golden_record() checks every single ID and prints a full report.
"""
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Literal

from core.hallucination_validator import apply_hallucination_guardrails


# ── All 163 expected parameters ───────────────────────────────────────────────
EXPECTED_PARAMETERS = {
    1:   ("Company Basics",          "Company Name",                     "Atomic"),
    2:   ("Company Basics",          "Short Name",                       "Atomic"),
    3:   ("Company Basics",          "Logo URL",                         "Composite"),
    4:   ("Company Basics",          "Category",                         "Atomic"),
    5:   ("Company Basics",          "Year of Incorporation",            "Atomic"),
    6:   ("Company Narrative",       "Overview of the Company",          "Atomic"),
    7:   ("Company Basics",          "Nature of Company",                "Atomic"),
    8:   ("Company Basics",          "Company Headquarters",             "Atomic"),
    9:   ("Geographic Presence",     "Countries Operating In",           "Composite"),
    10:  ("Geographic Presence",     "Number of Offices",                "Atomic"),
    11:  ("Geographic Presence",     "Office Locations",                 "Composite"),
    12:  ("People & Talent",         "Employee Size",                    "Atomic"),
    13:  ("People & Talent",         "Hiring Velocity",                  "Composite"),
    14:  ("People & Talent",         "Employee Turnover",                "Atomic"),
    15:  ("People & Talent",         "Average Retention Tenure",         "Atomic"),
    16:  ("Business Model",          "Pain Points Being Addressed",      "Composite"),
    17:  ("Business Model",          "Focus Sectors / Industries",       "Composite"),
    18:  ("Business Model",          "Services / Offerings / Products",  "Composite"),
    19:  ("Business Model",          "Top Customers by Client Segments", "Composite"),
    20:  ("Business Model",          "Core Value Proposition",           "Composite"),
    21:  ("Strategy & Culture",      "Vision",                           "Atomic"),
    22:  ("Strategy & Culture",      "Mission",                          "Atomic"),
    23:  ("Strategy & Culture",      "Values",                           "Composite"),
    24:  ("Strategy & Culture",      "Unique Differentiators",           "Composite"),
    25:  ("Strategy & Culture",      "Competitive Advantages",           "Composite"),
    26:  ("Strategy & Culture",      "Weaknesses / Gaps in Offering",    "Composite"),
    27:  ("Strategy & Culture",      "Key Challenges and Unmet Needs",   "Composite"),
    28:  ("Competitive Landscape",   "Key Competitors",                  "Composite"),
    29:  ("Competitive Landscape",   "Technology Partners",              "Composite"),
    30:  ("Company Narrative",       "Interesting Facts",                "Composite"),
    31:  ("Company Narrative",       "Recent News",                      "Composite"),
    32:  ("Digital Presence",        "Website URL",                      "Atomic"),
    33:  ("Digital Presence",        "Quality of Website",               "Atomic"),
    34:  ("Digital Presence",        "Website Rating",                   "Atomic"),
    35:  ("Digital Presence",        "Website Traffic Rank",             "Composite"),
    36:  ("Digital Presence",        "Social Media Followers Combined",  "Atomic"),
    37:  ("Digital Presence",        "Glassdoor Rating",                 "Atomic"),
    38:  ("Digital Presence",        "Indeed Rating",                    "Atomic"),
    39:  ("Digital Presence",        "Google Reviews Rating",            "Atomic"),
    40:  ("Digital Presence",        "LinkedIn Profile URL",             "Atomic"),
    41:  ("Digital Presence",        "Twitter X Handle",                 "Atomic"),
    42:  ("Digital Presence",        "Facebook Page URL",                "Atomic"),
    43:  ("Digital Presence",        "Instagram Page URL",               "Atomic"),
    44:  ("Leadership",              "CEO Name",                         "Atomic"),
    45:  ("Leadership",              "CEO LinkedIn URL",                 "Atomic"),
    46:  ("Leadership",              "Key Business Leaders",             "Composite"),
    47:  ("Leadership",              "Warm Introduction Pathways",       "Composite"),
    48:  ("Leadership",              "Decision Maker Accessibility",     "Atomic"),
    49:  ("Contact Info",            "Company Contact Email",            "Atomic"),
    50:  ("Contact Info",            "Company Phone Number",             "Atomic"),
    51:  ("Contact Info",            "Primary Contact Name",             "Atomic"),
    52:  ("Contact Info",            "Primary Contact Title",            "Atomic"),
    53:  ("Contact Info",            "Primary Contact Email",            "Atomic"),
    54:  ("Contact Info",            "Primary Contact Phone",            "Atomic"),
    55:  ("Reputation",              "Awards and Recognitions",          "Composite"),
    56:  ("Reputation",              "Brand Sentiment Score",            "Atomic"),
    57:  ("Reputation",              "Event Participation",              "Composite"),
    58:  ("Risk & Compliance",       "Regulatory Compliance Status",     "Composite"),
    59:  ("Risk & Compliance",       "Legal Issues Controversies",       "Atomic"),
    60:  ("Financials",              "Annual Revenues",                  "Atomic"),
    61:  ("Financials",              "Annual Profits",                   "Atomic"),
    62:  ("Financials",              "Revenue Mix",                      "Composite"),
    63:  ("Financials",              "Company Valuation",                "Atomic"),
    64:  ("Financials",              "Year-over-Year Growth Rate",       "Atomic"),
    65:  ("Financials",              "Profitability Status",             "Atomic"),
    66:  ("Financials",              "Market Share",                     "Atomic"),
    67:  ("Funding",                 "Key Investors Backers",            "Composite"),
    68:  ("Funding",                 "Recent Funding Rounds",            "Composite"),
    69:  ("Funding",                 "Total Capital Raised",             "Atomic"),
    70:  ("Sustainability",          "ESG Practices or Ratings",         "Composite"),
    71:  ("Sales & Growth",          "Sales Motion",                     "Atomic"),
    72:  ("Sales & Growth",          "Customer Acquisition Cost",        "Atomic"),
    73:  ("Sales & Growth",          "Customer Lifetime Value",          "Atomic"),
    74:  ("Sales & Growth",          "CAC LTV Ratio",                    "Atomic"),
    75:  ("Sales & Growth",          "Churn Rate",                       "Atomic"),
    76:  ("Sales & Growth",          "Net Promoter Score",               "Atomic"),
    77:  ("Sales & Growth",          "Customer Concentration Risk",      "Atomic"),
    78:  ("Sales & Growth",          "Burn Rate",                        "Atomic"),
    79:  ("Sales & Growth",          "Runway",                           "Atomic"),
    80:  ("Sales & Growth",          "Burn Multiplier",                  "Atomic"),
    81:  ("Innovation",              "Intellectual Property",            "Composite"),
    82:  ("Innovation",              "R&D Investment",                   "Atomic"),
    83:  ("Innovation",              "AI ML Adoption Level",             "Atomic"),
    84:  ("Operations",              "Tech Stack Tools Used",            "Composite"),
    85:  ("Operations",              "Cybersecurity Posture",            "Composite"),
    86:  ("Operations",              "Supply Chain Dependencies",        "Composite"),
    87:  ("Operations",              "Geopolitical Risks",               "Composite"),
    88:  ("Operations",              "Macro Risks",                      "Composite"),
    89:  ("People & Talent",         "Diversity Metrics",                "Composite"),
    90:  ("People & Talent",         "Remote Work Policy",               "Atomic"),
    91:  ("People & Talent",         "Training Development Spend",       "Atomic"),
    92:  ("Market",                  "Partnership Ecosystem",            "Composite"),
    93:  ("Market",                  "Exit Strategy History",            "Composite"),
    94:  ("Sustainability",          "Carbon Footprint",                 "Atomic"),
    95:  ("Sustainability",          "Ethical Sourcing Practices",       "Composite"),
    96:  ("Benchmarking",            "Benchmark vs Peers",               "Composite"),
    97:  ("Forecasting",             "Future Projections",               "Atomic"),
    98:  ("Forecasting",             "Strategic Priorities",             "Composite"),
    99:  ("Network",                 "Industry Associations",            "Composite"),
    100: ("Proof Points",            "Case Studies",                     "Composite"),
    101: ("Go-to-Market",            "Go-to-Market Strategy",            "Composite"),
    102: ("Innovation",              "Innovation Roadmap",               "Composite"),
    103: ("Innovation",              "Product Pipeline",                 "Composite"),
    104: ("Governance",              "Board of Directors Advisors",      "Composite"),
    105: ("Digital Presence",        "Company Marketing Videos",         "Composite"),
    106: ("Proof Points",            "Customer Testimonials",            "Composite"),
    107: ("Benchmarking",            "Industry Benchmark Tech Adoption", "Composite"),
    108: ("Market",                  "Total Addressable Market TAM",     "Atomic"),
    109: ("Market",                  "Serviceable Addressable Market",   "Atomic"),
    110: ("Market",                  "Serviceable Obtainable Market",    "Atomic"),
    111: ("Culture & People",        "Work Culture",                     "Composite"),
    112: ("Culture & People",        "Manager Quality",                  "Atomic"),
    113: ("Culture & People",        "Psychological Safety",             "Atomic"),
    114: ("Culture & People",        "Feedback Culture",                 "Composite"),
    115: ("Culture & People",        "Diversity Inclusion",              "Composite"),
    116: ("Culture & People",        "Ethical Standards",                "Composite"),
    117: ("Work-Life Balance",       "Typical Working Hours",            "Atomic"),
    118: ("Work-Life Balance",       "Overtime Expectations",            "Atomic"),
    119: ("Work-Life Balance",       "Weekend Work",                     "Atomic"),
    120: ("Work-Life Balance",       "Remote Hybrid Flexibility",        "Composite"),
    121: ("Work-Life Balance",       "Leave Policy",                     "Composite"),
    122: ("Work-Life Balance",       "Burnout Risk",                     "Atomic"),
    123: ("Location & Accessibility","Central vs Peripheral Location",   "Atomic"),
    124: ("Location & Accessibility","Public Transport Access",          "Composite"),
    125: ("Location & Accessibility","Cab Availability Policy",          "Composite"),
    126: ("Location & Accessibility","Commute Time from Airport",        "Atomic"),
    127: ("Location & Accessibility","Office Zone Type",                 "Atomic"),
    128: ("Safety & Well-being",     "Area Safety",                      "Composite"),
    129: ("Safety & Well-being",     "Company Safety Policies",          "Composite"),
    130: ("Safety & Well-being",     "Office Infrastructure Safety",     "Composite"),
    131: ("Safety & Well-being",     "Emergency Response Preparedness",  "Composite"),
    132: ("Safety & Well-being",     "Health Support",                   "Composite"),
    133: ("Learning & Growth",       "Onboarding Training Quality",      "Atomic"),
    134: ("Learning & Growth",       "Learning Culture",                 "Composite"),
    135: ("Learning & Growth",       "Exposure Quality",                 "Atomic"),
    136: ("Learning & Growth",       "Mentorship Availability",          "Composite"),
    137: ("Learning & Growth",       "Internal Mobility",                "Atomic"),
    138: ("Learning & Growth",       "Promotion Clarity",                "Composite"),
    139: ("Learning & Growth",       "Tools Technology Access",          "Composite"),
    140: ("Role & Work Quality",     "Role Clarity",                     "Atomic"),
    141: ("Role & Work Quality",     "Early Ownership",                  "Atomic"),
    142: ("Role & Work Quality",     "Work Impact",                      "Composite"),
    143: ("Role & Work Quality",     "Execution vs Thinking Balance",    "Atomic"),
    144: ("Role & Work Quality",     "Automation Level",                 "Atomic"),
    145: ("Role & Work Quality",     "Cross-functional Exposure",        "Composite"),
    146: ("Company Stability",       "Company Maturity",                 "Atomic"),
    147: ("Company Stability",       "Brand Value",                      "Atomic"),
    148: ("Company Stability",       "Client Quality",                   "Composite"),
    149: ("Company Stability",       "Layoff History",                   "Atomic"),
    150: ("Compensation & Benefits", "Fixed vs Variable Pay",            "Atomic"),
    151: ("Compensation & Benefits", "Bonus Predictability",             "Atomic"),
    152: ("Compensation & Benefits", "ESOPs Long-term Incentives",       "Composite"),
    153: ("Compensation & Benefits", "Family Health Insurance",          "Composite"),
    154: ("Compensation & Benefits", "Relocation Support",               "Composite"),
    155: ("Compensation & Benefits", "Lifestyle Wellness Benefits",      "Composite"),
    156: ("Long-Term Career",        "Exit Opportunities",               "Composite"),
    157: ("Long-Term Career",        "Skill Relevance",                  "Atomic"),
    158: ("Long-Term Career",        "External Recognition",             "Atomic"),
    159: ("Long-Term Career",        "Network Strength",                 "Composite"),
    160: ("Long-Term Career",        "Global Exposure",                  "Composite"),
    161: ("Values Alignment",        "Mission Clarity",                  "Atomic"),
    162: ("Values Alignment",        "Sustainability and CSR",           "Composite"),
    163: ("Values Alignment",        "Crisis Behavior",                  "Atomic"),
}

EMPTY_VALUES = {"not found", "n/a", "unknown", "none", "null", "", "-"}


# ── Single field Pydantic model ───────────────────────────────────────────────

class CompanyField(BaseModel):
    ID:        int
    Category:  str
    AC:        str  = Field(..., alias="A/C")
    Parameter: str
    Data:      str  = Field(..., alias="Research Output / Data")
    Source:    Optional[str] = None

    model_config = {"populate_by_name": True}

    @field_validator("ID")
    @classmethod
    def id_in_range(cls, v: int) -> int:
        if v < 1 or v > 163:
            raise ValueError(f"ID {v} out of range (1–163)")
        return v

    @field_validator("AC")
    @classmethod
    def ac_valid(cls, v: str) -> str:
        if v not in ("Atomic", "Composite"):
            raise ValueError(f"A/C must be 'Atomic' or 'Composite', got '{v}'")
        return v

    @field_validator("Data")
    @classmethod
    def data_not_empty(cls, v: str) -> str:
        if not v or v.strip().lower() in EMPTY_VALUES:
            return "Not Found"
        return v.strip()


# ── Per-field validation result ───────────────────────────────────────────────

class FieldValidationResult(BaseModel):
    ID:        int
    Category:  str
    AC:        str
    Parameter: str
    status:    Literal["✅ PASS", "⚠️  MISSING", "❌ FAIL"]
    issue:     Optional[str] = None


class HallucinationIssue(BaseModel):
    ID: int
    Parameter: str
    severity: Literal["warning", "critical"]
    rule: str
    message: str
    value: Optional[str] = None
    action: Literal["none", "normalized_to_not_found"] = "none"


# ── Validation report ─────────────────────────────────────────────────────────

class ValidationReport(BaseModel):
    company_name:     str
    total_expected:   int = 163
    total_received:   int
    total_passed:     int
    total_missing:    int
    total_failed:     int
    completeness_pct: float
    results:          List[FieldValidationResult]
    hallucination_issue_count: int = 0
    hallucination_critical_count: int = 0
    hallucination_sanitized_count: int = 0
    hallucination_issues: List[HallucinationIssue] = Field(default_factory=list)

    def print_report(self):
        sep = "=" * 70
        print(f"\n{sep}")
        print(f"  PYDANTIC VALIDATION REPORT — {self.company_name.upper()}")
        print(sep)
        print(f"  Expected     : {self.total_expected} parameters")
        print(f"  Received     : {self.total_received} rows from LLM")
        print(f"  ✅ Passed    : {self.total_passed}")
        print(f"  ⚠️  Missing   : {self.total_missing}")
        print(f"  ❌ Failed    : {self.total_failed}")
        print(f"  Completeness : {self.completeness_pct:.1f}%")
        print(f"  🚩 Hallucination flags: {self.hallucination_issue_count} "
              f"({self.hallucination_critical_count} critical, "
              f"{self.hallucination_sanitized_count} normalized)")
        print(sep)

        # Issues section
        issues = [r for r in self.results if r.status != "✅ PASS"]
        if issues:
            print(f"\n  ⚠️  ISSUES ({len(issues)} total):")
            print(f"  {'ID':>3}  {'Status':<14}  {'A/C':<10}  {'Parameter':<42}  Note")
            print("  " + "-" * 68)
            for r in issues:
                note = r.issue or ""
                print(f"  {r.ID:>3}  {r.status:<14}  {r.AC:<10}  {r.Parameter:<42}  {note}")

        if self.hallucination_issues:
            print(f"\n  🚩 HALLUCINATION FLAGS ({len(self.hallucination_issues)} total):")
            print(f"  {'ID':>3}  {'Severity':<9}  {'Rule':<34}  Action")
            print("  " + "-" * 68)
            for issue in self.hallucination_issues:
                print(
                    f"  {issue.ID:>3}  {issue.severity:<9}  {issue.rule:<34}  {issue.action}"
                )

        # Full per-field table
        print(f"\n  📋 FULL 163-FIELD DETAIL:")
        print(f"  {'ID':>3}  {'Category':<26}  {'A/C':<10}  {'Parameter':<42}  Status")
        print("  " + "-" * 98)
        for r in self.results:
            print(f"  {r.ID:>3}  {r.Category:<26}  {r.AC:<10}  {r.Parameter:<42}  {r.status}")

        print(f"\n{sep}")
        print(f"  ✅ {self.total_passed}/163 validated  |  {self.completeness_pct:.1f}% complete")
        print(f"{sep}\n")


# ── Main validation function ──────────────────────────────────────────────────

def validate_golden_record(
    raw_data: list,
    company_name: str
) -> tuple:
    """
    Validates all 163 expected parameters.
    Returns: (valid_rows: list, report: ValidationReport)

    For each of 163 IDs:
      ✅ PASS    — row exists and passes Pydantic
      ⚠️  MISSING — ID not in golden record
      ❌ FAIL    — row exists but failed validation
    """
    # Index by ID
    incoming = {}
    for row in raw_data:
        id_ = row.get("ID")
        if id_ is not None:
            incoming[int(id_)] = row

    results    = []
    valid_rows = []
    passed = missing = failed = 0

    for id_ in range(1, 164):
        exp_cat, exp_param, exp_ac = EXPECTED_PARAMETERS[id_]

        if id_ not in incoming:
            results.append(FieldValidationResult(
                ID=id_, Category=exp_cat, AC=exp_ac, Parameter=exp_param,
                status="⚠️  MISSING", issue="Not returned by LLM"
            ))
            missing += 1
            continue

        row = incoming[id_]
        try:
            # If LLM put garbage in A/C (e.g. company name), use expected value
            raw_ac = row.get("A/C", "")
            ac_val = raw_ac if raw_ac in ("Atomic", "Composite") else exp_ac

            # Try multiple key names the LLM might use for the data field
            data_val = (
                row.get("Research Output / Data")
                or row.get("Research Output")
                or row.get("Data")
                or "Not Found"
            )

            field = CompanyField.model_validate({
                "ID":                     id_,
                "A/C":                    ac_val,
                "Category":               row.get("Category") or exp_cat,
                "Parameter":              row.get("Parameter") or exp_param,
                "Research Output / Data": data_val,
                "Source":                 row.get("Source", ""),
            })
            results.append(FieldValidationResult(
                ID=id_, Category=field.Category, AC=field.AC,
                Parameter=field.Parameter, status="✅ PASS"
            ))
            valid_rows.append(field.model_dump(by_alias=True))
            passed += 1

        except Exception as e:
            results.append(FieldValidationResult(
                ID=id_, Category=exp_cat, AC=exp_ac, Parameter=exp_param,
                status="❌ FAIL", issue=str(e)
            ))
            failed += 1

    valid_rows, hallucination_issues, sanitized_count = apply_hallucination_guardrails(valid_rows)
    critical_count = sum(1 for issue in hallucination_issues if issue["severity"] == "critical")

    report = ValidationReport(
        company_name=company_name,
        total_received=len(incoming),
        total_passed=passed,
        total_missing=missing,
        total_failed=failed,
        completeness_pct=round((passed / 163) * 100, 1),
        results=results,
        hallucination_issue_count=len(hallucination_issues),
        hallucination_critical_count=critical_count,
        hallucination_sanitized_count=sanitized_count,
        hallucination_issues=[HallucinationIssue.model_validate(x) for x in hallucination_issues],
    )
    return valid_rows, report
