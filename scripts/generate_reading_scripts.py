"""MeetMind AI — Human Reading Script Generator (B2).

Generates clean, printable markdown scripts for human voice recording sessions
from the exact same scenarios as B1, with numbers written phonetically as spoken.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "ml" / "eval" / "audio" / "synthetic"))

from scenarios import SCENARIOS, VOICE_MAP

OUTPUT_DIR = REPO_ROOT / "docs" / "eval" / "reading_scripts"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    master_script_path = OUTPUT_DIR / "READING_SCRIPTS_MASTER.md"

    with open(master_script_path, "w", encoding="utf-8") as mf:
        mf.write("# MeetMind AI — Human Audio Recording Session Scripts (Tier B)\n\n")
        mf.write("> **Speaker Instructions**:\n")
        mf.write("> - 3 Speakers: Speaker A (Partner), Speaker B (Principal), Speaker C (Founder/Legal).\n")
        mf.write("> - Read naturally at conversational executive pace (130–160 words per minute).\n")
        mf.write("> - Speak numbers exactly as spelled out phonetically.\n")
        mf.write("> - Record in a quiet room (first pass), then repeat in room with background noise (second pass).\n\n")
        mf.write("---\n\n")

        for sc_name, turns in SCENARIOS.items():
            sc_title = sc_name.replace("_", " ").title()
            mf.write(f"## {sc_title}\n\n")

            sc_file = OUTPUT_DIR / f"{sc_name}.md"
            with open(sc_file, "w", encoding="utf-8") as sf:
                sf.write(f"# {sc_title}\n\n")
                sf.write("**Scenario Context**: Executive board & IC discussions involving Indian corporate finances.\n\n")

                for idx, turn in enumerate(turns, 1):
                    role = VOICE_MAP[turn.speaker]["role"]
                    gender = turn.gender
                    lang = "English" if turn.language_tag == "en" else "Hinglish (Code-Switched)"

                    block = (
                        f"### Turn {idx:02d}: {turn.speaker.upper()} ({role} — {gender} — {lang})\n\n"
                        f"> \"{turn.text}\"\n\n"
                    )
                    if turn.canonical_numbers:
                        block += f"*Numbers Spoken*: {', '.join(n['raw_token'] for n in turn.canonical_numbers)}\n\n"
                    if turn.entities:
                        block += f"*Key Entities*: {', '.join(turn.entities)}\n\n"

                    sf.write(block)
                    mf.write(block)

            mf.write("---\n\n")

    print(f"Human reading scripts generated in {OUTPUT_DIR}")
    print(f"Master script: {master_script_path}")


if __name__ == "__main__":
    main()

