"""MeetMind AI — Human Reading Script Generator (B2 / Corpus v2).

Generates clean, printable markdown scripts for human voice recording sessions
from the exact same scenarios as B1, with numbers written phonetically as spoken.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def main():
    parser = argparse.ArgumentParser(description="Generate Human Reading Scripts")
    parser.add_argument(
        "--corpus",
        choices=["eng", "finance"],
        default="eng",
        help="Domain scenarios to render (eng|finance, default: eng)",
    )
    args = parser.parse_args()

    if args.corpus == "eng":
        from ml.eval.audio.synthetic_eng.scenarios import SCENARIOS, VOICE_MAP

        output_dir = REPO_ROOT / "docs" / "eval" / "reading_scripts"
        context_str = (
            "Engineering team discussions involving architectural and operational decisions."
        )
    else:
        from ml.eval.audio.synthetic.scenarios import SCENARIOS, VOICE_MAP

        output_dir = REPO_ROOT / "docs" / "eval" / "reading_scripts_finance"
        context_str = "Executive board & IC discussions involving Indian corporate finances."

    output_dir.mkdir(parents=True, exist_ok=True)
    master_script_path = output_dir / "READING_SCRIPTS_MASTER.md"

    with open(master_script_path, "w", encoding="utf-8") as mf:
        mf.write(
            f"# MeetMind AI — Human Audio Recording Session Scripts [{args.corpus.upper()}]\n\n"
        )
        mf.write("> **Speaker Instructions**:\n")
        mf.write(
            "> - 4 Speakers: Tech Lead (Male), EM (Female), Senior SRE (Female), "
            "Platform Eng (Male).\n"
        )
        mf.write(
            "> - Read naturally at conversational executive pace (130–160 words per minute).\n"
        )
        mf.write(
            "> - Speak numbers, versions, and identifiers exactly as spelled out phonetically.\n"
        )
        mf.write(
            "> - Record in a quiet room (first pass), then repeat in room with background noise "
            "(second pass).\n\n"
        )
        mf.write("---\n\n")

        for sc_name, turns in SCENARIOS.items():
            sc_title = sc_name.replace("_", " ").title()
            mf.write(f"## {sc_title}\n\n")

            sc_file = output_dir / f"{sc_name}.md"
            with open(sc_file, "w", encoding="utf-8") as sf:
                sf.write(f"# {sc_title}\n\n")
                sf.write(f"**Scenario Context**: {context_str}\n\n")

                for idx, turn in enumerate(turns, 1):
                    voice_info = VOICE_MAP.get(turn.speaker, {})
                    role = voice_info.get("role", "Speaker")
                    gender = turn.gender
                    lang = "English" if turn.language_tag == "en" else "Hinglish (Code-Switched)"

                    block = (
                        f"### Turn {idx:02d}: {turn.speaker.upper()} "
                        f"({role} — {gender} — {lang})\n\n"
                        f'> "{turn.text}"\n\n'
                    )
                    if turn.canonical_numbers:
                        nums_str = ", ".join(n["raw_token"] for n in turn.canonical_numbers)
                        block += f"*Numbers Spoken*: {nums_str}\n\n"
                    if turn.entities:
                        if isinstance(turn.entities[0], dict):
                            ent_str = ", ".join(f"{e['text']} ({e['type']})" for e in turn.entities)
                        else:
                            ent_str = ", ".join(turn.entities)
                        block += f"*Key Entities*: {ent_str}\n\n"
                    if turn.decisions:
                        block += f"*Decisions*: {', '.join(turn.decisions)}\n\n"
                    if turn.action_items:
                        items_str = "; ".join(
                            f"{a.get('owner')}: {a.get('task')} (Due: {a.get('due')})"
                            for a in turn.action_items
                        )
                        block += f"*Action Items*: {items_str}\n\n"

                    sf.write(block)
                    mf.write(block)

            mf.write("---\n\n")

    print(f"Human reading scripts generated in {output_dir}")
    print(f"Master script: {master_script_path}")


if __name__ == "__main__":
    main()
