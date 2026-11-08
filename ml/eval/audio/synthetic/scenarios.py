"""MeetMind AI — Synthetic Corpus Scenario Definitions (B1).

Defines 8 scenarios (~30 minutes total speech) containing:
- 4 distinct speakers (2 male, 2 female Indian English voices)
- Indian numbering (lakh, crore, spoken forms)
- % and bps, rupee and dollar amounts
- Dates
- 50+ real listed Indian companies & invented entities
- Hinglish code-switched turns
- Deliberate ASR traps (EBITDA, INR 12 crore vs 2 crore, etc.)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Turn:
    speaker: str  # "speaker_0", "speaker_1", "speaker_2", "speaker_3"
    voice: str  # Voice name
    gender: str  # "Male" or "Female"
    language_tag: str  # "en" or "hinglish"
    text: str  # Text to synthesize
    canonical_numbers: list[dict[str, Any]]
    entities: list[str]


# 4 Distinct Indian Voices (2 Male, 2 Female)
VOICE_MAP = {
    "speaker_0": {"name": "en-IN-PrabhatNeural", "gender": "Male", "role": "Partner / Lead"},
    "speaker_1": {"name": "en-IN-NeerjaNeural", "gender": "Female", "role": "Principal / CFO"},
    "speaker_2": {"name": "hi-IN-MadhurNeural", "gender": "Male", "role": "Founder / CEO"},
    "speaker_3": {"name": "hi-IN-SwaraNeural", "gender": "Female", "role": "Legal Counsel / Director"},
}

SCENARIOS: dict[str, list[Turn]] = {
    "scenario_01_investment_committee": [
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="Welcome everyone to the investment committee meeting. Today we are evaluating ZetaGrid Technologies for a Series B growth round.",
            canonical_numbers=[],
            entities=["ZetaGrid Technologies", "Series B"],
        ),
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="Thank you. The proposed round size is INR 120 crore, which translates to roughly 14.4 million USD at the current exchange rate.",
            canonical_numbers=[
                {"canonical_value": 1200000000.0, "unit": "INR", "raw_token": "120 crore"},
                {"canonical_value": 14400000.0, "unit": "USD", "raw_token": "14.4 million"},
            ],
            entities=["ZetaGrid Technologies"],
        ),
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="hinglish",
            text="Hamari annual recurring revenue 45 crore tak pahunch gayi hai, and we expect EBITDA margin to reach 18 percent by March 2027.",
            canonical_numbers=[
                {"canonical_value": 450000000.0, "unit": "COUNT", "raw_token": "45 crore"},
                {"canonical_value": 18.0, "unit": "%", "raw_token": "18 percent"},
            ],
            entities=["EBITDA"],
        ),
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="en",
            text="The post-money valuation cap is set at INR 850 crore, with a 150 bps discount on the existing convertible notes.",
            canonical_numbers=[
                {"canonical_value": 8500000000.0, "unit": "INR", "raw_token": "850 crore"},
                {"canonical_value": 150.0, "unit": "BPS", "raw_token": "150 bps"},
            ],
            entities=["SAFE"],
        ),
    ],
    "scenario_02_term_sheet": [
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="en",
            text="Let us review the term sheet. Clause 4 specifies a 1.0x non-participating liquidation preference for Series B preference shares.",
            canonical_numbers=[{"canonical_value": 1.0, "unit": "COUNT", "raw_token": "1.0x"}],
            entities=["Series B", "Clause 4"],
        ),
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="We require the ESOP pool to be expanded to 12 percent unallocated prior to investment, with standard 4-year monthly vesting and a 1-year cliff.",
            canonical_numbers=[
                {"canonical_value": 12.0, "unit": "%", "raw_token": "12 percent"},
                {"canonical_value": 4.0, "unit": "COUNT", "raw_token": "4-year"},
                {"canonical_value": 1.0, "unit": "COUNT", "raw_token": "1-year"},
            ],
            entities=["ESOP"],
        ),
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="hinglish",
            text="Yeh terms acceptable hain, but ROFR and co-sale rights must not restrict secondary sales under two crore rupees.",
            canonical_numbers=[{"canonical_value": 20000000.0, "unit": "INR", "raw_token": "two crore rupees"}],
            entities=["ROFR"],
        ),
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="Agreed, provided representations and warranties indemnity is capped at fifteen percent of total consideration for 18 months.",
            canonical_numbers=[
                {"canonical_value": 15.0, "unit": "%", "raw_token": "fifteen percent"},
                {"canonical_value": 18.0, "unit": "COUNT", "raw_token": "18 months"},
            ],
            entities=[],
        ),
    ],
    "scenario_03_covenant_negotiation": [
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="Turning to debt covenants, the consortium led by State Bank of India and HDFC Bank requires a maximum debt-to-equity ratio of 2.5x.",
            canonical_numbers=[{"canonical_value": 2.5, "unit": "COUNT", "raw_token": "2.5x"}],
            entities=["State Bank of India", "HDFC Bank"],
        ),
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="Our debt service coverage ratio currently stands at 1.45x, safely above their minimum 1.35x covenant trigger.",
            canonical_numbers=[
                {"canonical_value": 1.45, "unit": "COUNT", "raw_token": "1.45x"},
                {"canonical_value": 1.35, "unit": "COUNT", "raw_token": "1.35x"},
            ],
            entities=["DSCR"],
        ),
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="hinglish",
            text="ICICI Bank aur Axis Bank ne INR 25 crore ka cash escrow liquidity buffer manga hai to approve the term loan facility.",
            canonical_numbers=[{"canonical_value": 250000000.0, "unit": "INR", "raw_token": "INR 25 crore"}],
            entities=["ICICI Bank", "Axis Bank"],
        ),
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="en",
            text="We can fund the INR 25 crore buffer directly from operational cash flows without drawing on Kotak Mahindra lines.",
            canonical_numbers=[{"canonical_value": 250000000.0, "unit": "INR", "raw_token": "INR 25 crore"}],
            entities=["Kotak Mahindra Bank"],
        ),
    ],
    "scenario_04_mnpi_slip": [
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="Please note this discussion contains material non-public information. Bharti Airtel has formally approached IndusPulse for a strategic acquisition.",
            canonical_numbers=[],
            entities=["Bharti Airtel", "IndusPulse", "MNPI"],
        ),
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="The non-binding offer is priced at Rs. 150 crore, representing a 35 percent premium over their last unlisted share price.",
            canonical_numbers=[
                {"canonical_value": 1500000000.0, "unit": "INR", "raw_token": "Rs. 150 crore"},
                {"canonical_value": 35.0, "unit": "%", "raw_token": "35 percent"},
            ],
            entities=["IndusPulse"],
        ),
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="hinglish",
            text="Yeh deal confidential hai, and the formal regulatory filing with SEBI will happen on November 18, 2026.",
            canonical_numbers=[{"canonical_value": 18.0, "unit": "COUNT", "raw_token": "18"}],
            entities=["SEBI", "November 18, 2026"],
        ),
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="en",
            text="Their unannounced Q3 revenue beat consensus by 22 percent, with EBITDA surging to twelve crore rupees.",
            canonical_numbers=[
                {"canonical_value": 22.0, "unit": "%", "raw_token": "22 percent"},
                {"canonical_value": 120000000.0, "unit": "INR", "raw_token": "twelve crore rupees"},
            ],
            entities=["EBITDA", "Q3"],
        ),
    ],
    "scenario_05_commitments_with_owners": [
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="hinglish",
            text="Q4 ke commitments clear hain: we have signed multi-year enterprise contracts with Tata Consultancy Services and Infosys.",
            canonical_numbers=[],
            entities=["Tata Consultancy Services", "Infosys", "Q4"],
        ),
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="Reliance Industries and Larsen & Toubro are also in advanced pilot stages, with expected ACV of fifty lakh rupees each.",
            canonical_numbers=[{"canonical_value": 5000000.0, "unit": "INR", "raw_token": "fifty lakh rupees"}],
            entities=["Reliance Industries", "Larsen & Toubro", "ACV"],
        ),
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="To support these accounts, we have committed to hiring 30 senior AI engineers across Bengaluru and Hyderabad by January 15.",
            canonical_numbers=[
                {"canonical_value": 30.0, "unit": "COUNT", "raw_token": "30"},
                {"canonical_value": 15.0, "unit": "COUNT", "raw_token": "15"},
            ],
            entities=["Bengaluru", "Hyderabad", "January 15"],
        ),
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="en",
            text="Wipro and HCL Technologies have signed master services agreements, guaranteeing minimum billable hours for 24 months.",
            canonical_numbers=[{"canonical_value": 24.0, "unit": "COUNT", "raw_token": "24 months"}],
            entities=["Wipro", "HCL Technologies", "MSA"],
        ),
    ],
    "scenario_06_objection": [
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="I must raise a serious objection regarding your gross margin profile. Net churn increased from 2.0 percent to 4.5 percent last quarter.",
            canonical_numbers=[
                {"canonical_value": 2.0, "unit": "%", "raw_token": "2.0 percent"},
                {"canonical_value": 4.5, "unit": "%", "raw_token": "4.5 percent"},
            ],
            entities=["Churn"],
        ),
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="hinglish",
            text="Chur ki wajah BharatEdge AI ki aggressive discounting thi; they offered enterprise pricing with a 40 bps margin cut.",
            canonical_numbers=[{"canonical_value": 40.0, "unit": "BPS", "raw_token": "40 bps"}],
            entities=["BharatEdge AI"],
        ),
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="However, our net retention rate remains resilient at 114 percent across tier-one clients like Sun Pharma and Titan Company.",
            canonical_numbers=[{"canonical_value": 114.0, "unit": "%", "raw_token": "114 percent"}],
            entities=["Sun Pharma", "Titan Company", "NRR"],
        ),
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="en",
            text="Our enterprise clients sign non-cancellable 36-month contracts with annual 5 percent step-up escalations.",
            canonical_numbers=[
                {"canonical_value": 36.0, "unit": "COUNT", "raw_token": "36-month"},
                {"canonical_value": 5.0, "unit": "%", "raw_token": "5 percent"},
            ],
            entities=[],
        ),
    ],
    "scenario_07_competitor_mention": [
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="Looking at competitive dynamics, VedaCommerce and Adani Enterprises are entering the platform intelligence space.",
            canonical_numbers=[],
            entities=["VedaCommerce", "Adani Enterprises"],
        ),
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="hinglish",
            text="VedaCommerce ka solution legacy systems pe based hai, whereas hamara stack UltraTech Cement aur Asian Paints me already deployed hai.",
            canonical_numbers=[],
            entities=["VedaCommerce", "UltraTech Cement", "Asian Paints"],
        ),
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="Note the distinction between Tata Consumer Products and Tata Communications in your pipeline; their contract values differ by Rs. 150 crore versus Rs. 12 crore.",
            canonical_numbers=[
                {"canonical_value": 1500000000.0, "unit": "INR", "raw_token": "Rs. 150 crore"},
                {"canonical_value": 120000000.0, "unit": "INR", "raw_token": "Rs. 12 crore"},
            ],
            entities=["Tata Consumer Products", "Tata Communications"],
        ),
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="en",
            text="Bajaj Finance and Maruti Suzuki have renewed their agreements at an expanded run-rate of eighty lakh rupees annually.",
            canonical_numbers=[{"canonical_value": 8000000.0, "unit": "INR", "raw_token": "eighty lakh rupees"}],
            entities=["Bajaj Finance", "Maruti Suzuki"],
        ),
    ],
    "scenario_08_valuation_discussion": [
        Turn(
            speaker="speaker_0",
            voice="en-IN-PrabhatNeural",
            gender="Male",
            language_tag="en",
            text="Let us conclude with the discounted cash flow valuation model. We assumed a weighted average cost of capital of 13.5 percent.",
            canonical_numbers=[{"canonical_value": 13.5, "unit": "%", "raw_token": "13.5 percent"}],
            entities=["WACC", "DCF"],
        ),
        Turn(
            speaker="speaker_1",
            voice="en-IN-NeerjaNeural",
            gender="Female",
            language_tag="en",
            text="With a terminal growth rate of 5.0 percent, the implied equity value lands at INR 920 crore, benchmarked against NTPC and Power Grid multiples.",
            canonical_numbers=[
                {"canonical_value": 5.0, "unit": "%", "raw_token": "5.0 percent"},
                {"canonical_value": 9200000000.0, "unit": "INR", "raw_token": "INR 920 crore"},
            ],
            entities=["NTPC", "Power Grid"],
        ),
        Turn(
            speaker="speaker_2",
            voice="hi-IN-MadhurNeural",
            gender="Male",
            language_tag="hinglish",
            text="Yeh valuation justifiable hai because hamara ARR growth rate 65 percent per annum hai, significantly higher than ONGC or ITC.",
            canonical_numbers=[{"canonical_value": 65.0, "unit": "%", "raw_token": "65 percent"}],
            entities=["ONGC", "ITC", "ARR"],
        ),
        Turn(
            speaker="speaker_3",
            voice="hi-IN-SwaraNeural",
            gender="Female",
            language_tag="en",
            text="The board will formally adopt this valuation model at the extraordinary general meeting scheduled for December 22, 2026.",
            canonical_numbers=[{"canonical_value": 22.0, "unit": "COUNT", "raw_token": "22"}],
            entities=["EGM", "December 22, 2026"],
        ),
    ],
}

